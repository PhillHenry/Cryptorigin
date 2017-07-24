package uk.co.odinconsultants.bitcoin.integration

import java.nio.ByteBuffer

import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes.toBytes
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.bitcoin.hbase.HBaseMetaRetrieval.toAddress
import uk.co.odinconsultants.bitcoin.hbase.HBaseSetup._
import uk.co.odinconsultants.bitcoin.hbase.{HBaseMetaRetrieval, HBaseMetaStore}

@RunWith(classOf[JUnitRunner])
class HBaseMetaStoreIntegrationSpec extends WordSpec with Matchers {

  "HBase" should {
    "be there for integration tests" in {
      val configuration = HBaseConfiguration.create()
      configuration.set("fs.file.impl", classOf[LocalFileSystem].getName)
      configuration.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
      val utility = new org.apache.hadoop.hbase.HBaseTestingUtility(configuration)
      utility.startMiniCluster()

      val admin           = utility.getHBaseAdmin

      createAddressesTable(admin)

      val table           = admin.getConnection.getTable(tableName)

      val inserter        = new HBaseMetaStore(table, familyName)

      val hash            = toBytes("rowkey1")
      val index           = 42
      val rawAddress      = Array.fill(20)(0.toByte)
      val expectedAddress = toAddress(rawAddress)


      inserter((hash, index), expectedAddress)

      val selector        = new HBaseMetaRetrieval(table, familyName)
      val actualAddress   = selector(hash, index)

      actualAddress shouldEqual expectedAddress

      utility.shutdownMiniCluster()
    }
  }

  private def select(table: HTableInterface, key: ByteBuffer) = {
    val aGet    = new Get(key.array())
    val result  = table.get(aGet)
    result.value()
  }

}
