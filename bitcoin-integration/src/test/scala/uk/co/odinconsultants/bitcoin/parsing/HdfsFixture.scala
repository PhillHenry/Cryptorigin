package uk.co.odinconsultants.bitcoin.parsing

import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.bitcoin.apps.SparkBlockChain.blockChainRdd
import uk.co.odinconsultants.bitcoin.core.Logging
import uk.co.odinconsultants.bitcoin.hbase.HBaseMetaRetrieval
import uk.co.odinconsultants.bitcoin.hbase.HBaseSetup.{createAddressesTable, familyName, metaTable}
import uk.co.odinconsultants.bitcoin.integration.hadoop.MiniHadoopClusterRunning
import uk.co.odinconsultants.bitcoin.integration.hbase.HBaseForTesting.{admin, utility}
import uk.co.odinconsultants.bitcoin.integration.hbase.HBaseTestConfig.getConnection
import uk.co.odinconsultants.bitcoin.integration.spark.SparkForTesting.sc
import uk.co.odinconsultants.bitcoin.parsing.Indexer.{index, write}

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

    "have its metadata persisted in HBase" in {
      createAddressesTable(admin)
      write(outputs, () => getConnection(utility.getConfiguration))

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
  }
}
