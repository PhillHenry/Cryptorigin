package uk.co.odinconsultants.bitcoin.integration

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}

@RunWith(classOf[JUnitRunner])
class HBaseSmokeTestSpec extends WordSpec with Matchers {

  "HBase" should {
    "be there for integration tests" in {
      val configuration = org.apache.hadoop.hbase.HBaseConfiguration.create()
      configuration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
      configuration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
      val utility = new org.apache.hadoop.hbase.HBaseTestingUtility(configuration)
      utility.startMiniCluster()
      utility.shutdownMiniCluster()
    }
  }

}
