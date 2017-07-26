package uk.co.odinconsultants.bitcoin.hbase

import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.HBaseAdmin

object HBaseSetup {

  val metaTable: String   = "Addresses"
  val tableName: TableName = TableName.valueOf(metaTable)
  val familyName: String  = "familyName"

  def createAddressesTable(admin: HBaseAdmin): Unit = {
    createTable(admin, metaTable, familyName)
  }

  private def createTable(admin: HBaseAdmin, tableName: String, familyName: String): Unit = {
    val tableDescriptor = new HTableDescriptor(tableName)
    val colDescriptor   = new HColumnDescriptor(familyName)
    tableDescriptor.addFamily(colDescriptor)
    if (!admin.tableExists(tableName)) {
      admin.createTable(tableDescriptor)
    }
  }

}
