package uk.co.odinconsultants.bitcoin.hbase

import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}

object HBaseSetup {

  val metaTable: String   = "Addresses"
  val tableName: TableName = TableName.valueOf(metaTable)
  val familyName: String  = "familyName"

  def connection(): Connection = ConnectionFactory.createConnection()

  def createAddressesTable(admin: Admin): Unit =
    createTable(admin, tableName, familyName)

  private def createTable(admin: Admin, tableName: TableName, familyName: String): Unit = {
    val tableDescriptor = new HTableDescriptor(tableName)
    val colDescriptor   = new HColumnDescriptor(familyName)
    tableDescriptor.addFamily(colDescriptor)
    if (!admin.tableExists(tableName)) {
      admin.createTable(tableDescriptor)
    }
  }

}
