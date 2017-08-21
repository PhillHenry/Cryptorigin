package uk.co.odinconsultants.bitcoin.parsing

import org.apache.commons.codec.binary.Hex
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.zuinnote.hadoop.bitcoin.format.common.{BitcoinTransaction, BitcoinTransactionInput, BitcoinTransactionOutput}
import scala.collection.JavaConversions._

trait TestDomainObjects extends MockitoSugar {

  def script(i: Int): Array[Byte]    = Hex.decodeHex("a914".toCharArray) ++ Array.fill[Byte](20)(i.toByte) ++ Hex.decodeHex("87".toCharArray)
  val someBytes: Array[Byte] = Array.fill[Byte](10)(0)

  def createInputX(n: Int): Seq[BitcoinTransactionInput] = {
    val prevTxHash    = Array.fill[Byte](20)(0)
    (0 until n).map { i =>
      val input         = mock[BitcoinTransactionInput]
      when(input.getPreviousTxOutIndex).thenReturn(i)
      when(input.getPrevTransactionHash).thenReturn(prevTxHash)
      when(input.getTxInScript).thenReturn(someBytes)
      when(input.getTxInScriptLength).thenReturn(someBytes)
      input
    }
  }

  def createOutputX(n: Int): Seq[BitcoinTransactionOutput] = {
    (0 until n).map { i =>
      val output      = mock[BitcoinTransactionOutput]
      when(output.getTxOutScript).thenReturn(script(i))
      when(output.getTxOutScriptLength).thenReturn(someBytes)
      output
    }
  }

  def createTransaction(inputs: Seq[BitcoinTransactionInput], outputs: Seq[BitcoinTransactionOutput]) : BitcoinTransaction = {
    val tx = mock[BitcoinTransaction]
    when(tx.getInCounter).thenReturn(someBytes) // no idea how realistic this is
    when(tx.getOutCounter).thenReturn(someBytes) // no idea how realistic this is
    when(tx.getListOfInputs).thenReturn(inputs)
    when(tx.getListOfOutputs).thenReturn(outputs)
    tx
  }

}
