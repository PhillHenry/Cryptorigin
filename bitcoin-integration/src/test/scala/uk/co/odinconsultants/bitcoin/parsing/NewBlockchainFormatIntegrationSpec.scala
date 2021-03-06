package uk.co.odinconsultants.bitcoin.parsing

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.bitcoin.integration.hadoop.MiniHadoopClusterRunning

@RunWith(classOf[JUnitRunner])
class NewBlockchainFormatIntegrationSpec  extends WordSpec with Matchers with MiniHadoopClusterRunning with HdfsFixture {

  override def filename: String = "multiblock.blk"

}