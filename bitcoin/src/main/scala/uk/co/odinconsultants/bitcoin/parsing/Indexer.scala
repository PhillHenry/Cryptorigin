package uk.co.odinconsultants.bitcoin.parsing

import org.apache.hadoop.io.BytesWritable
import org.apache.spark.rdd.RDD
import org.bitcoinj.core._
import org.bitcoinj.params.MainNetParams
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlock
import uk.co.odinconsultants.bitcoin.parsing.DomainOps._

import scala.collection.JavaConversions._

object Indexer {

  type PubKey         = Address
  type BackReference  = (Array[Byte], Long)

  val networkParams: MainNetParams = MainNetParams.get()

  def index(rdd: RDD[(BytesWritable, BitcoinBlock)]): RDD[PubKey] = // TODO these mappings must be persisted
    rdd.flatMap(toTransactions).flatMap(_.getListOfOutputs).flatMap(toOutputAddress)

}
