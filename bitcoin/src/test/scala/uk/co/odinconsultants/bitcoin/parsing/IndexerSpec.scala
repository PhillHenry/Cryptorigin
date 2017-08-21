package uk.co.odinconsultants.bitcoin.parsing

import org.apache.commons.codec.binary.Hex
import org.junit.runner.RunWith
import org.mockito.Mockito.when
import org.scalatest.junit.JUnitRunner
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}
import org.zuinnote.hadoop.bitcoin.format.common.{BitcoinTransaction, BitcoinTransactionInput, BitcoinTransactionOutput}
import uk.co.odinconsultants.bitcoin.hbase.HBaseMetaRetrieval
import uk.co.odinconsultants.bitcoin.parsing.DomainOps.toPublicKey

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class IndexerSpec extends WordSpec with Matchers with MockitoSugar {

  import Indexer._

  "A transaction" should {
    "be a mapping of inputs to outputs" in {
      val someBytes     = Array.fill[Byte](10)(0)

      val input         = mock[BitcoinTransactionInput]
      val prevIndx      = 0
      val prevTxHash    = Array.fill[Byte](20)(0)
      when(input.getPreviousTxOutIndex).thenReturn(prevIndx)
      when(input.getPrevTransactionHash).thenReturn(prevTxHash)
      when(input.getTxInScript).thenReturn(someBytes)
      when(input.getTxInScriptLength).thenReturn(someBytes)

      val bytes       = Hex.decodeHex("a9149c79163af51f480446f5b4943d774476d305a0bb87".toCharArray)
      val output      = mock[BitcoinTransactionOutput]
      when(output.getTxOutScript).thenReturn(bytes)
      when(output.getTxOutScriptLength).thenReturn(someBytes)

      val tx          = mock[BitcoinTransaction]
      when(tx.getInCounter).thenReturn(someBytes) // no idea how realistic this is
      when(tx.getOutCounter).thenReturn(someBytes) // no idea how realistic this is
      when(tx.getListOfInputs).thenReturn(List(input))
      when(tx.getListOfOutputs).thenReturn(List(output))

      val store       = mock[HBaseMetaRetrieval]
      val storedKey   = Array.fill[Byte](20)(1)
      when(store(List((prevTxHash, prevIndx)))).thenReturn(List(storedKey))

      val graph       = cartesianProductOfIO(tx, store)
      graph should have size 1
      graph should contain (hashed(storedKey), hashed(toPublicKey(bytes).get))
    }
  }

}
