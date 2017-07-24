package uk.co.odinconsultants.bitcoin.integration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.client.HBaseAdmin

trait HBaseTesting {

  val configuration: Configuration = HBaseConfiguration.create()
  configuration.set("fs.file.impl", classOf[LocalFileSystem].getName)
  configuration.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)

  val utility           = new HBaseTestingUtility(configuration)
  utility.startMiniCluster()
  val admin: HBaseAdmin = utility.getHBaseAdmin
}
