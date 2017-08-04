package uk.co.odinconsultants.bitcoin.uk.co.odinconsultants.bitcoin.parsing

import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.bitcoin.integration.hadoop.MiniHadoopClusterRunning

class NewBlockchainFormatIntegrationSpec  extends WordSpec with Matchers with MiniHadoopClusterRunning with HdfsFixture {

  override def filename: String = "multiblock.blk"

}