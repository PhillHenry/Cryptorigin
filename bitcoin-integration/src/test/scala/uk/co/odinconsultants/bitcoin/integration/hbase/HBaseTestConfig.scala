package uk.co.odinconsultants.bitcoin.integration.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hdfs.DistributedFileSystem

object HBaseTestConfig {

  val configuration: Configuration = HBaseConfiguration.create()
  configuration.set("fs.file.impl", classOf[LocalFileSystem].getName)
  configuration.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)

  def getConnection(configuration: Configuration): Connection = ConnectionFactory.createConnection(configuration)

}
