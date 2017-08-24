package uk.co.odinconsultants.bitcoin.integration.hbase

import java.net.ServerSocket

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.client.HBaseAdmin
import uk.co.odinconsultants.bitcoin.integration.hbase.HBaseTestConfig.configuration

object HBaseForTesting {

//  val ss = new ServerSocket(0)
//  val zkPort = ss.getLocalPort

  val utility           = new HBaseTestingUtility(configuration)
//  utility.getConfiguration.set("test.hbase.zookeeper.property.clientPort", s"${zkPort}")
  utility.startMiniCluster()
  val admin: HBaseAdmin = utility.getHBaseAdmin

}
