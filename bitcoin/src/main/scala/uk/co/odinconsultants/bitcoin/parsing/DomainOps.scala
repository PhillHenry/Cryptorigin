package uk.co.odinconsultants.bitcoin.parsing

import org.apache.hadoop.io.BytesWritable
import org.bitcoinj.script.Script
import org.zuinnote.hadoop.bitcoin.format.common.{BitcoinBlock, BitcoinTransaction, BitcoinTransactionInput, BitcoinTransactionOutput}
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinUtil.getTransactionHash
import uk.co.odinconsultants.bitcoin.parsing.Indexer.{BackReference, PubKey, networkParams}

import scala.collection.JavaConversions._

object DomainOps {

  val toTransactions: ((BytesWritable, BitcoinBlock)) => Seq[BitcoinTransaction] = { case (_, block) => block.getTransactions }

  val toBackReference: (BitcoinTransactionInput) => BackReference = { in => (in.getPrevTransactionHash, in.getPreviousTxOutIndex) }

  def hashOf(tx: BitcoinTransaction): Array[Byte] = getTransactionHash(tx)

  val toOutputAddress: (BitcoinTransactionOutput) => TraversableOnce[PubKey] = { case (txOutput) =>
    val script = new Script(txOutput.getTxOutScript)

    if (script.isSentToAddress) Some(script.getToAddress(networkParams))
    else if (script.isPayToScriptHash) Some(script.getToAddress(networkParams))
    else None
  }

  val toBackReferenceAddressTuples: (BitcoinTransaction) => Seq[(BackReference, PubKey)] = { tx =>
    val txHash = hashOf(tx)
    tx.getListOfOutputs.zipWithIndex.flatMap { case (out, i) => toOutputAddress(out).map(addr => (txHash, i.toLong) -> addr) }
  }

}
