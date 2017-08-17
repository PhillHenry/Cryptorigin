package uk.co.odinconsultants.bitcoin.hbase

import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}
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

  def toTableName(name: String): TableName = TableName.valueOf(name)

  def createAddressesTable(admin: Admin): Unit =
    createTable(admin, tableName, familyName)

  def createTable(admin: Admin, tableName: TableName, familyName: String): Unit = {
    val tableDescriptor = new HTableDescriptor(tableName)
    val colDescriptor   = new HColumnDescriptor(familyName)
    tableDescriptor.addFamily(colDescriptor)
    if (!admin.tableExists(tableName)) {
      admin.createTable(tableDescriptor)
    }
  }

}
