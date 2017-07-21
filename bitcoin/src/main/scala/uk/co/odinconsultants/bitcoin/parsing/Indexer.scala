package uk.co.odinconsultants.bitcoin.parsing

import org.apache.hadoop.io.BytesWritable
import org.apache.spark.rdd.RDD
import org.bitcoinj.core._
import org.bitcoinj.params.MainNetParams
import org.bitcoinj.script.Script
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinUtil.getTransactionHash
import org.zuinnote.hadoop.bitcoin.format.common.{BitcoinBlock, BitcoinTransaction, BitcoinTransactionInput, BitcoinTransactionOutput}

import scala.collection.JavaConversions._

object Indexer {

  type PubKey = Address

  type BackReference = (Array[Byte], Long)

  val networkParams: MainNetParams = MainNetParams.get()

  val toTransactions: ((BytesWritable, BitcoinBlock)) => Seq[BitcoinTransaction] = { case (_, block) => block.getTransactions }

  val toBackReference: (BitcoinTransactionInput) => BackReference = { in => (in.getPrevTransactionHash, in.getPreviousTxOutIndex) }

  val backReferenceToAddress: (BitcoinTransaction) => Seq[(BackReference, PubKey)] = { tx =>
    val txHash = hashOf(tx)
    tx.getListOfOutputs.zipWithIndex.flatMap { case (out, i) => toOutputAddress(out).map(addr => (txHash, i.toLong) -> addr) }
  }

  def hashOf(tx: BitcoinTransaction): Array[Byte] = getTransactionHash(tx)

  val toOutputAddress: (BitcoinTransactionOutput) => TraversableOnce[PubKey] = { case (txOutput) =>
    val script = new Script(txOutput.getTxOutScript)

    if (script.isSentToAddress) Some(script.getToAddress(networkParams))
    else if (script.isPayToScriptHash) Some(script.getToAddress(networkParams))
    else None
  }

  def index(rdd: RDD[(BytesWritable, BitcoinBlock)]): RDD[PubKey] =
    rdd.flatMap(toTransactions).flatMap(_.getListOfOutputs).flatMap(toOutputAddress)

}
