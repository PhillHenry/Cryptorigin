package uk.co.odinconsultants.bitcoin.parsing

import org.junit.runner.RunWith
import org.mockito.Mockito.when
import org.scalatest.junit.JUnitRunner
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.bitcoin.hbase.HBaseMetaRetrieval
import uk.co.odinconsultants.bitcoin.parsing.DomainOps.toPublicKey

@RunWith(classOf[JUnitRunner])
class IndexerSpec extends WordSpec with Matchers with MockitoSugar with TestDomainObjects {

  import Indexer._

  "A payment" should {

    "map all inputs to outputs" in {
      val inputs      = createInputX(10)
      val outputs     = createOutputX(9)
      val tx          = createTransaction(inputs, outputs)
      val store       = mock[HBaseMetaRetrieval]
      val storedKeys  = inputs.zipWithIndex.map { case (x, i) => Array.fill[Byte](20)(i.toByte) }.toList
      when(store(inputs.map(x => (x.getPrevTransactionHash, x.getPreviousTxOutIndex)).toList)).thenReturn(storedKeys)

      val graph       = cartesianProductOfIO(tx, store).distinct
      graph should have size (inputs.size * outputs.size)
      for (i <- inputs.indices) {
        for (o <- outputs.indices) {
          graph should contain (hashed(storedKeys(i)), hashed(toPublicKey(script(o)).get))
        }
      }
    }

    "map an input to an output" in {
      val inputs      = createInputX(1)
      val outputs     = createOutputX(1)
      val tx          = createTransaction(inputs, outputs)
      val store       = mock[HBaseMetaRetrieval]
      val storedKey   = Array.fill[Byte](20)(1)
      when(store(inputs.map(x => (x.getPrevTransactionHash, x.getPreviousTxOutIndex)).toList)).thenReturn(List(storedKey))

      val graph       = cartesianProductOfIO(tx, store)
      graph should have size 1
      graph should contain (hashed(storedKey), hashed(toPublicKey(script(0)).get))
    }
  }

}
