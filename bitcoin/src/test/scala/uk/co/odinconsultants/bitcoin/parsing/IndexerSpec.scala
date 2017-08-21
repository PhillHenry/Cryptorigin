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

  "A transaction" should {
    "be a mapping of inputs to outputs" in {
      val inputs      = createInputX(1)
      val outputs     = createOutputX(1)
      val tx          = createTransaction(inputs, outputs)
      val store       = mock[HBaseMetaRetrieval]
      val storedKey   = Array.fill[Byte](20)(1)
      when(store(inputs.map(x => (x.getPrevTransactionHash, x.getPreviousTxOutIndex)).toList)).thenReturn(List(storedKey))

      val graph       = cartesianProductOfIO(tx, store)
      graph should have size 1
      graph should contain (hashed(storedKey), hashed(toPublicKey(script).get))
    }
  }

}
