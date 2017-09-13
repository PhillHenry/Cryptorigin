package uk.co.odinconsultants.bitcoin.hbase

import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import uk.co.odinconsultants.bitcoin.core.Logging

/**
  * "An HBase table contains column families, which are the logical and physical grouping of columns. There are column
  * qualifiers inside of a column family, which are the columns. Column families contain columns with time stamped
  * versions. Columns only exist when they are inserted, which makes HBase a sparse database. All column members of the
  * same column family have the same column family prefix."
  * https://www.ibm.com/support/knowledgecenter/en/SSPT3X_4.1.0/com.ibm.swg.im.infosphere.biginsights.analyze.doc/doc/hbaseConcepts.html
  */
object HBaseSetup extends Logging {

  val metaTable: String   = "Addresses"
  val tableName: TableName = toTableName(metaTable)
  val familyName: String  = "fN"
  val qualifier: String   = "AQ"

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
  }

  def createTable(admin: Admin, tableName: TableName, familyName: String): (HTableDescriptor, HColumnDescriptor) = {
    val tableDescriptor = new HTableDescriptor(tableName)
    val col   = new HColumnDescriptor(familyName)
    col.setCacheDataInL1(true)
    col.setBloomFilterType(BloomType.ROW)
    col.setCompressionType(Compression.Algorithm.GZ)
    col.setCompactionCompressionType(Compression.Algorithm.GZ)
    col.setBlocksize(16 * 1024) // better for random access as less is pulled into (and consequently evicted) from the cache
    tableDescriptor.addFamily(col)
    if (!admin.tableExists(tableName)) {
      admin.createTable(tableDescriptor, Array(0.toByte), Array(255.toByte), 20)
    }
    (tableDescriptor, col)
  }

}
