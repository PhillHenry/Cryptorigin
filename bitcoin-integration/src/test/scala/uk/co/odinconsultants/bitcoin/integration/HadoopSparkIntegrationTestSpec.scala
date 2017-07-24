package uk.co.odinconsultants.bitcoin.integration

import org.apache.hadoop.io.BytesWritable
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlock
import org.zuinnote.hadoop.bitcoin.format.mapreduce.BitcoinBlockFileInputFormat
import uk.co.odinconsultants.bitcoin.integration.hadoop.MiniHadoopClusterRunning
import uk.co.odinconsultants.bitcoin.integration.spark.SparkForTesting.sc
import uk.co.odinconsultants.bitcoin.parsing.Indexer

@RunWith(classOf[JUnitRunner])
class HadoopSparkIntegrationTestSpec extends WordSpec with Matchers with MiniHadoopClusterRunning {

  "Hadoop mini cluster" should {
    "hold Hadoop files" in {
      val hdfsFile = copyToHdfs(localFile("multiblock.blk"))

      val files = list("/")
      files should have size 1

      val rdd = sc.newAPIHadoopFile(hdfsFile.toString, classOf[BitcoinBlockFileInputFormat], classOf[BytesWritable], classOf[BitcoinBlock], conf)
      rdd.count() should be > 0L

      val outputs = Indexer.index(rdd)
      outputs.count() should be > 0L
    }
  }

}
