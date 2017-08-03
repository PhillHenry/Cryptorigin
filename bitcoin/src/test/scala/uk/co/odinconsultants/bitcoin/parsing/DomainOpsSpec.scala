package uk.co.odinconsultants.bitcoin.parsing

import org.scalatest.{Matchers, WordSpec}

class DomainOpsSpec extends WordSpec with Matchers {

  import DomainOps._

  private val oldScript = Array[Byte](65, 4, 103, -118, -3, -80, -2, 85, 72, 39, 25, 103, -15, -90, 113, 48, -73, 16, 92, -42,
    -88, 40, -32, 57, 9, -90, 121, 98, -32, -22, 31, 97, -34, -74, 73, -10, -68, 63, 76, -17, 56, -60, -13, 85, 4, -27,
    30, -63, 18, -34, 92, 56, 77, -9, -70, 11, -115, 87, -118, 76, 112, 43, 107, -15, 29, 95, -84)

  "Format 1 (according to http://codesuppository.blogspot.co.at/2014/01/how-to-parse-bitcoin-blockchain.html)" should {
    "be parsed" in {
      val option = toPublicKey(oldScript)
      option should not be None
      option.foreach { value =>
        value should have length 20
      }
    }
  }

}
