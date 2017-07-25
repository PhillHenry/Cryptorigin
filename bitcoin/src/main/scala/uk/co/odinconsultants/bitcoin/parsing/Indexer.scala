package uk.co.odinconsultants.bitcoin.parsing

import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.rdd.RDD
import org.bitcoinj.core._
import org.bitcoinj.params.MainNetParams
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlock
import uk.co.odinconsultants.bitcoin.hbase.HBaseMetaStore
import uk.co.odinconsultants.bitcoin.hbase.HBaseSetup.{familyName, tableName}
import uk.co.odinconsultants.bitcoin.parsing.DomainOps._
import uk.co.odinconsultants.bitcoin.parsing.MetaStore.Payload

object Indexer {

  type PubKey         = Address
  type BackReference  = (Array[Byte], Long)

  val networkParams: MainNetParams = MainNetParams.get()

  def index(rdd: RDD[(BytesWritable, BitcoinBlock)]): RDD[Payload] =
    rdd.flatMap(toTransactions).flatMap(toBackReferenceAddressTuples)

  def write(rdd: RDD[Payload], connection: Connection): Unit = {
    val batchSize = 100
    rdd.foreachPartition { iter =>
      val table     = connection.getTable(tableName)
      val metaStore = new HBaseMetaStore(table, familyName)

      iter.grouped(batchSize).foreach { metaIter =>
        metaStore(metaIter.toList)
      }
    }
  }

}
