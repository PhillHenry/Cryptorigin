package uk.co.odinconsultants.bitcoin.integration.hbase

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.client.HBaseAdmin
import uk.co.odinconsultants.bitcoin.integration.hbase.HBaseTestConfig.configuration

object HBaseForTesting {

  val utility           = new HBaseTestingUtility(configuration)
  utility.startMiniCluster()
  val admin: HBaseAdmin = utility.getHBaseAdmin

}
