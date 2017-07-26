package uk.co.odinconsultants.bitcoin.hbase

import org.bitcoinj.core.Address
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.bitcoin.hbase.HBaseMetaRetrieval.toAddress
import uk.co.odinconsultants.bitcoin.parsing.Indexer

import scala.util.Random

class PubKeySerializationSpec extends WordSpec with Matchers {

  "Converting to and from an address" should {
    "give you an equal object" in {
      val random          = new Random()
      val rawAddress      = Array.fill(20)(0.toByte)
      random.nextBytes(rawAddress)
      val expectedAddress = new Address(Indexer.networkParams, rawAddress)
      expectedAddress shouldBe toAddress(HBaseMetaStore.serialize(expectedAddress))
    }
  }

}
