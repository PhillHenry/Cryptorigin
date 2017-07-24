package uk.co.odinconsultants.bitcoin.integration.hbase

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.client.HBaseAdmin

trait HBaseRunning {

  val utility: HBaseTestingUtility  = HBaseForTesting.utility
  val admin: HBaseAdmin             = HBaseForTesting.admin

}
