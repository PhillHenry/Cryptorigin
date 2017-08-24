package uk.co.odinconsultants.bitcoin.hbase

import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import uk.co.odinconsultants.bitcoin.core.Logging

object HBaseSetup extends Logging {

  val metaTable: String   = "Addresses"
  val tableName: TableName = toTableName(metaTable)
  val familyName: String  = "familyName"

  def connection(): Connection = ConnectionFactory.createConnection()

  def tableExists(name: String, admin: Admin): Boolean = admin.tableExists(toTableName(name))

  def dropTable(name: String, admin: Admin): Unit = {
    val tableName = toTableName(name)
    if (admin.isTableEnabled(tableName)) {
      info(s"Disabling $name")
      admin.flush(tableName)
      admin.compact(tableName)
      admin.disableTable(tableName)
    } else {
      info(s"Table $name already disabled")
    }
    info(s"Deleting $name")
    admin.deleteTable(tableName)
    info(s"Deleted $name")
  }

  def prepareMetaTable(refresh: Boolean, conn: Connection): Unit = {
    val admin = conn.getAdmin
    if (tableExists(metaTable, admin)) {
      if (refresh) {
        dropTable(metaTable, admin)
        createAddressesTable(admin)
      }
    } else {
      createAddressesTable(admin)
    }
  }

  def toTableName(name: String): TableName = TableName.valueOf(name)

  def createAddressesTable(admin: Admin): Unit = {
    val (table, col) = createTable(admin, tableName, familyName)
    col.setCacheDataInL1(true)
    col.setBloomFilterType(BloomType.ROW)
    col.setCompressionType(Compression.Algorithm.GZ)
    col.setCompactionCompressionType(Compression.Algorithm.GZ)
    col.setBlocksize(16 * 1024) // better for random access as less is pulled into (and consequently evicted) from the cache
  }

  def createTable(admin: Admin, tableName: TableName, familyName: String): (HTableDescriptor, HColumnDescriptor) = {
    val tableDescriptor = new HTableDescriptor(tableName)
    val colDescriptor   = new HColumnDescriptor(familyName)
    tableDescriptor.addFamily(colDescriptor)
    if (!admin.tableExists(tableName)) {
      admin.createTable(tableDescriptor, Array(0.toByte), Array(255.toByte), 20)
    }
    (tableDescriptor, colDescriptor)
  }

}
