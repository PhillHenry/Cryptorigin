package uk.co.odinconsultants.bitcoin.parsing

import org.apache.hadoop.io.BytesWritable
import org.apache.spark.rdd.RDD
import org.bitcoinj.core._
import org.bitcoinj.params.MainNetParams
import org.bitcoinj.script.Script
import org.zuinnote.hadoop.bitcoin.format.common.{BitcoinBlock, BitcoinTransaction, BitcoinTransactionOutput}

import scala.collection.JavaConversions._

object Indexer {

  type PubKey = Address

  val networkParams: MainNetParams = MainNetParams.get()

  val toTransaction: ((BytesWritable, BitcoinBlock)) => Seq[BitcoinTransaction] = { case (_, block) => block.getTransactions }

  val toOutputAddress: (BitcoinTransactionOutput) => TraversableOnce[PubKey] = { case (txOutput) =>
    val script = new Script(txOutput.getTxOutScript)

    if (script.isSentToAddress) Some(script.getToAddress(networkParams))
    else if (script.isPayToScriptHash) Some(script.getToAddress(networkParams))
    else None
  }

  def index(rdd: RDD[(BytesWritable, BitcoinBlock)]): RDD[PubKey] = {
    rdd.flatMap(toTransaction).flatMap(_.getListOfOutputs).flatMap(toOutputAddress)
  }

}
