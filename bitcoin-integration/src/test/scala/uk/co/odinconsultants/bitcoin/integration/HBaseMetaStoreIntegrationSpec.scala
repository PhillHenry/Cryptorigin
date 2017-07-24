package uk.co.odinconsultants.bitcoin.integration

import java.nio.ByteBuffer

import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes.toBytes
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.bitcoinj.core.Address
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.bitcoin.hbase.HBaseMetaStore
import uk.co.odinconsultants.bitcoin.parsing.Indexer
import uk.co.odinconsultants.bitcoin.parsing.Indexer.networkParams

@RunWith(classOf[JUnitRunner])
class HBaseMetaStoreIntegrationSpec extends WordSpec with Matchers {

  "HBase" should {
    "be there for integration tests" in {
      val configuration = HBaseConfiguration.create()
      configuration.set("fs.file.impl", classOf[LocalFileSystem].getName)
      configuration.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
      val utility = new org.apache.hadoop.hbase.HBaseTestingUtility(configuration)
      utility.startMiniCluster()

      val admin       = utility.getHBaseAdmin

      val tableName   = "mytable"
      val familyName  = "familyName"
      createTable(admin, tableName, familyName)

      val table       = admin.getConnection.getTable(tableName)

      val inserter    = new HBaseMetaStore(table, familyName)

      val hash        = toBytes("rowkey1")
      val index       = 42
      val actual      = Array.fill(20)(0.toByte)
      val address     = Address.fromP2SHHash(networkParams, actual)

      inserter((hash, index), address)

      val value       = select(table, HBaseMetaStore.append(hash, index))
      value shouldEqual actual

      utility.shutdownMiniCluster()
    }
  }

  private def select(table: HTableInterface, key: ByteBuffer) = {
    val aGet    = new Get(key.array())
    val result  = table.get(aGet)
    result.value()
  }

  private def createTable(admin: HBaseAdmin, tableName: String, familyName: String) = {
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    val colDescriptor   = new HColumnDescriptor(familyName)
    tableDescriptor.addFamily(colDescriptor)
    admin.createTable(tableDescriptor)
  }
}
