package uk.co.odinconsultants.bitcoin.uk.co.odinconsultants.bitcoin.parsing

import org.apache.hadoop.io.BytesWritable
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlock
import org.zuinnote.hadoop.bitcoin.format.mapreduce.BitcoinBlockFileInputFormat
import uk.co.odinconsultants.bitcoin.hbase.HBaseMetaRetrieval
import uk.co.odinconsultants.bitcoin.hbase.HBaseSetup._
import uk.co.odinconsultants.bitcoin.integration.hadoop.MiniHadoopClusterRunning
import uk.co.odinconsultants.bitcoin.integration.hbase.HBaseForTesting.{admin, utility}
import uk.co.odinconsultants.bitcoin.integration.hbase.HBaseTestConfig.getConnection
import uk.co.odinconsultants.bitcoin.integration.spark.SparkForTesting.sc
import uk.co.odinconsultants.bitcoin.parsing.Indexer._

@RunWith(classOf[JUnitRunner])
class HadoopSparkIntegrationTestSpec extends WordSpec with Matchers with MiniHadoopClusterRunning {

  "Copied file to HDFS" should {
    val hdfsFile = copyToHdfs(localFile("multiblock.blk"))

    "be possible" in {
      val files = list("/")
      files should have size 1
    }

    val rdd = sc.newAPIHadoopFile(hdfsFile.toString, classOf[BitcoinBlockFileInputFormat], classOf[BytesWritable], classOf[BitcoinBlock], conf)
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
        val actual                  = reader(backReference)
        withClue(s"\nWrote = $pubKey (${pubKey.mkString(",")})\nRead = $actual (${actual.mkString(",")})\n") {
          actual shouldEqual pubKey
        }
      }
    }
  }

}
