package uk.co.odinconsultants.bitcoin.integration

import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes.toBytes
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}

@RunWith(classOf[JUnitRunner])
class HBaseSmokeTestSpec extends WordSpec with Matchers {

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
      val key         = "rowkey1"
      val actual      = "value"

      insert(table, familyName, key, actual)

      val value       = select(table, key)
      value shouldEqual toBytes(actual)

      utility.shutdownMiniCluster()
    }
  }

  private def select(table: HTableInterface, key: String) = {
    val aGet    = new Get(toBytes(key))
    val result  = table.get(aGet)
    result.value()
  }

  private def insert(table: HTableInterface, familyName: String, key: String, actual: String) = {
    val aPut = new Put(toBytes(key))
    aPut.addColumn(toBytes(familyName), Array(), toBytes(actual))
    table.put(aPut)
  }

  private def createTable(admin: HBaseAdmin, tableName: String, familyName: String) = {
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    val colDescriptor   = new HColumnDescriptor(familyName)
    tableDescriptor.addFamily(colDescriptor)
    admin.createTable(tableDescriptor)
  }
}
