package uk.co.odinconsultants.bitcoin.parsing

import org.bitcoinj.script.Script
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinUtil.getTransactionHash
import org.zuinnote.hadoop.bitcoin.format.common._
import uk.co.odinconsultants.bitcoin.parsing.Indexer.{BackReference, PubKey, networkParams}

import scala.collection.JavaConversions._

object DomainOps {

  val toTransactions: (BitcoinBlock) => Seq[BitcoinTransaction] = _.getTransactions

  val toHash: (BitcoinBlock) => Array[Byte] = { bb =>
    BitcoinUtil.getBlockHash(bb)
  }

  val toBackReference: (BitcoinTransactionInput) => BackReference = { in =>
    (in.getPrevTransactionHash, in.getPreviousTxOutIndex)
  }

  def hashOf(tx: BitcoinTransaction): Array[Byte] = getTransactionHash(tx)

  val toOutputAddress: (BitcoinTransactionOutput) => TraversableOnce[PubKey] = { case (txOutput) =>
    val script = new Script(txOutput.getTxOutScript)

    if (script.isSentToAddress) Some(script.getToAddress(networkParams))
    else if (script.isPayToScriptHash) Some(script.getToAddress(networkParams))
    else None
  }

  val toBackReferenceAddressTuples: (BitcoinTransaction) => Seq[(BackReference, PubKey)] = { tx =>
    val hash = hashOf(tx)
    tx.getListOfOutputs.zipWithIndex.flatMap { case (out, i) => toOutputAddress(out).map(addr => (hash, i.toLong) -> addr) }
  }

}
