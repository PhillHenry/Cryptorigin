package uk.co.odinconsultants.bitcoin.integration

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}

@RunWith(classOf[JUnitRunner])
class HadoopSmokeTestSpec extends WordSpec with Matchers with MiniDfsClusterRunning {

  "Hadoop mini cluster" should {
    "hold Hadoop files" in {
      copy("multiblock.blk", "test.blk")
      list("/") should have size 1
    }
  }

}
