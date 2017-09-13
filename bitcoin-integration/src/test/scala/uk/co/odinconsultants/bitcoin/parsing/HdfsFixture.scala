package uk.co.odinconsultants.bitcoin.parsing

import java.lang.Math.pow

import maths.ChiSquareUtils
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Scan
import org.apache.spark.SparkConfigUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation, SparkHBaseConf}
import org.scalatest.{Matchers, WordSpec}
import org.zuinnote.hadoop.bitcoin.format.common.{BitcoinTransaction, BitcoinTransactionInput}
import uk.co.odinconsultants.bitcoin.apps.SparkBlockChain.blockChainRdd
import uk.co.odinconsultants.bitcoin.core.Logging
import uk.co.odinconsultants.bitcoin.hbase.{HBaseMetaRetrieval, HBaseSetup}
import uk.co.odinconsultants.bitcoin.hbase.HBaseSetup.{createAddressesTable, familyName, metaTable}
import uk.co.odinconsultants.bitcoin.integration.hadoop.MiniHadoopClusterRunning
import uk.co.odinconsultants.bitcoin.integration.hbase.HBaseForTesting
import uk.co.odinconsultants.bitcoin.integration.hbase.HBaseForTesting.{admin, utility}
import uk.co.odinconsultants.bitcoin.integration.hbase.HBaseTestConfig.getConnection
import uk.co.odinconsultants.bitcoin.integration.spark.SparkForTesting
import uk.co.odinconsultants.bitcoin.integration.spark.SparkForTesting._
import uk.co.odinconsultants.bitcoin.parsing.DomainOps.toOutputAddress
import uk.co.odinconsultants.bitcoin.parsing.Indexer.{index, toGraph, transactionsOf, write}

import scala.collection.JavaConversions._

trait HdfsFixture extends MiniHadoopClusterRunning with Matchers with Logging { this: WordSpec =>

  def filename: String

  "Copied file to HDFS" should {
    info(s"Using blockchain file: '$filename'")
    val hdfsFile = copyToHdfs(localFile(filename))

    "be possible" in {
      val files = list(dir)
      info(s"Files = ${files.mkString(", ")}")
      files should have size 1
    }

    val rdd = blockChainRdd(sc, hdfsFile.toString, conf)
    "allow Spark to use it" in {
      rdd.count() should be > 0L
    }

    val outputs = index(rdd)
    "not generated dupes when indexed" in {
      outputs.count() should be > 0L
      val dupes = outputs.map(_ -> 1).reduceByKey(_ + _).filter(_._2 > 1).collect()
      withClue(s"\n${dupes.mkString("\n")}\n") {
        dupes shouldBe empty
      }
    }

    val txFactory = () => getConnection(utility.getConfiguration)

    "have its metadata persisted in HBase" in {
      createAddressesTable(admin)
      write(outputs, txFactory)

      val reader = new HBaseMetaRetrieval(admin.getConnection.getTable(metaTable), familyName)
      outputs.collect().foreach { payload =>
        val (backReference, pubKey) = payload
        val actual                  = reader(List(backReference))
        withClue(s"\nWrote = $pubKey (${pubKey.mkString(",")})\nRead = $actual (${actual.mkString(",")})\n") {
          actual.size shouldBe 1
          actual.head shouldEqual pubKey
        }
      }
    }

    "be well distributed" in {
      val table   = admin.getConnection.getTable(metaTable)
      val results = table.getScanner(new Scan())
      var result  = results.next()
      var count   = 0
      val distn   = scala.collection.mutable.Map[Byte, Int]().withDefault(_ => 0)
      while (result != null) {
        val row = result.getRow
        val key = row(0)
        val next = distn(key) + 1
        distn += key -> next
        count += 1
        result = results.next()
      }
      count shouldBe > (0)
      val sum         = distn.values.sum
      sum shouldBe count
      val mean        = sum.toDouble / distn.size
      val chiSquared  = distn.values.map(x => pow(x - mean, 2) / mean).sum
      val pNull       = 1 - ChiSquareUtils.pochisq(chiSquared, count - 1)
      info(s"chiSquared = $chiSquared, pNull = $pNull")
      withClue(s"mean = $mean, |buckets| = ${distn.size}, count = $count, chiSquared = $chiSquared, pNull = $pNull\n${distn.toSeq.sortBy(_._1).map(x => f"${x._1}%-5s: ${"#" * x._2}%-50s (${x._2})").mkString("\n")}\n") {
        pNull shouldBe < (0.01) // ie, test asserts that the chances of a fluke must be less than 1%
      }
    }

    "be parsed using the persisted metadata" in {
      val realTxs     = transactionsOf(rdd)
      val rddTxsInDb  = realTxs.map(HdfsFixture.inputsPointToSelf)
      val adjacency   = toGraph(rddTxsInDb, txFactory).collect
      adjacency.length shouldBe > (realTxs.count().toInt)
    }

    "produce an address mapping" in {
      val sparkSession    = SparkSession.builder().master(master).appName(appName).getOrCreate()
      import sparkSession.sqlContext.implicits._
      SparkConfigUtil.conf(sparkSession, SparkHBaseConf.testConf, true.toString)
      SparkHBaseConf.conf = HBaseForTesting.utility.getConfiguration

      val df              = Indexer.catalogueAddresses(sparkSession)
      val rows            = df.collect()
      rows.length should be > 0

      val outputPKs = rows.map(_.get(1)).map(_.asInstanceOf[Array[Byte]])
      val inputPKs  = outputs.map(_._2).collect()

      // make sure we're dealing with PK hashes
      outputPKs.foreach { pk => pk.length shouldBe 20}
      inputPKs.foreach { pk => pk.length shouldBe 20}

      withClue(s"\nSample of PKs persisted = ${inputPKs.take(10).map(Hex.encodeHexString).mkString(", ")}\nSample of PKs extracted = ${outputPKs.take(10).map(x => Hex.encodeHexString(x)).mkString(", ")}\n") {
        outputPKs.distinct.length shouldBe inputPKs.distinct.length
      }
    }
  }
}

object HdfsFixture extends Logging {

  val inputsPointToSelf: BitcoinTransaction => BitcoinTransaction = { tx =>
    val myHash = DomainOps.hashOf(tx)
    info(s"Faking input of tx with ${tx.getListOfInputs.size()} inputs and ${tx.getListOfOutputs.size}")
    val newInputs = tx.getListOfInputs.flatMap { i =>
      val addresses = tx.getListOfOutputs.flatMap(toOutputAddress).zipWithIndex
      if (addresses.isEmpty)
        None
      else
        Some(new BitcoinTransactionInput(myHash, addresses.head._2, i.getTxInScriptLength, i.getTxInScript, i.getSeqNo))
    }
    new BitcoinTransaction(tx.getVersion, tx.getInCounter, newInputs, tx.getOutCounter, tx.getListOfOutputs, tx.getLockTime)
  }

}
