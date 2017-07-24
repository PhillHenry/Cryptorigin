package uk.co.odinconsultants.bitcoin.hbase

import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.HBaseAdmin

object HBaseSetup {

  val tableName: String   = "Addresses"
  val familyName: String  = "familyName"

  def createAddressesTable(admin: HBaseAdmin): Unit = {
    createTable(admin, tableName, familyName)
  }

  private def createTable(admin: HBaseAdmin, tableName: String, familyName: String): Unit = {
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    val colDescriptor   = new HColumnDescriptor(familyName)
    tableDescriptor.addFamily(colDescriptor)
    admin.createTable(tableDescriptor)
  }

}
