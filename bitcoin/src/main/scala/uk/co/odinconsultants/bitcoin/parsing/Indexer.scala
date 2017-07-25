package uk.co.odinconsultants.bitcoin.parsing

import org.apache.hadoop.io.BytesWritable
import org.apache.spark.rdd.RDD
import org.bitcoinj.core._
import org.bitcoinj.params.MainNetParams
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlock
import uk.co.odinconsultants.bitcoin.parsing.DomainOps._

object Indexer {

  type PubKey         = Address
  type BackReference  = (Array[Byte], Long)

  val networkParams: MainNetParams = MainNetParams.get()

  def index(rdd: RDD[(BytesWritable, BitcoinBlock)]): RDD[(BackReference, PubKey)] =
    rdd.flatMap(toTransactions).flatMap(toBackReferenceAddressTuples)

}
