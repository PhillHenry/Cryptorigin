package uk.co.odinconsultants.bitcoin.hbase

import org.scalatest.{Matchers, WordSpec}

import scala.Array.emptyByteArray

class HBaseMetaStoreSpec extends WordSpec with Matchers {

  import HBaseMetaStore._

  "appending" should {
    "have the correct number of bytes" in {
      append(emptyByteArray, 42L).array should have length 8
    }
  }

}
