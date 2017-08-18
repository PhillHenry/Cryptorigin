package uk.co.odinconsultants.bitcoin.parsing

import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.rdd.RDD
import org.bitcoinj.params.MainNetParams
import org.zuinnote.hadoop.bitcoin.format.common.{BitcoinBlock, BitcoinTransaction}
import uk.co.odinconsultants.bitcoin.hbase.HBaseSetup.{familyName, tableName}
import uk.co.odinconsultants.bitcoin.hbase.{HBaseMetaRetrieval, HBaseMetaStore}
import uk.co.odinconsultants.bitcoin.parsing.DomainOps._
import uk.co.odinconsultants.bitcoin.parsing.MetaStore.Payload

import scala.collection.JavaConversions._

object Indexer {

  type PubKey         = Array[Byte]
  type BackReference  = (Array[Byte], Long)

  val networkParams: MainNetParams = MainNetParams.get()

  def index(rdd: RDD[(BytesWritable, BitcoinBlock)]): RDD[Payload] =
    rdd.flatMap{ case(_, block) => toTransactions(block) }.flatMap(toBackReferenceAddressTuples)

  def write(rdd: RDD[Payload], connectionFactory: () => Connection): Unit = {
    val batchSize = 100
    rdd.foreachPartition { iter =>
      val connection  = connectionFactory()
      val table       = connection.getTable(tableName)
      val metaStore   = new HBaseMetaStore(table, familyName)

      iter.grouped(batchSize).foreach { metaIter =>
        metaStore(metaIter.toList)
      }

      connection.close()
    }
  }

  /**
    * TODO not used yet. Refactor and test. This is just a brain dump.
    */
  def toGraph(rdd: RDD[BitcoinTransaction], connectionFactory: () => Connection): RDD[(Long, Long)] = {
    rdd.mapPartitions { txs =>
      val connection  = connectionFactory()
      val table       = connection.getTable(tableName)
      val metaStore   = new HBaseMetaRetrieval(table, familyName)

      val payments = txs.flatMap { tx =>
        val inputs = tx.getListOfInputs
        val backRefs = inputs.map { i =>
          (i.getPrevTransactionHash, i.getPreviousTxOutIndex)
        }
        val pkInputs = metaStore(backRefs.toList)

        val pkOutputs = toBackReferenceAddressTuples(tx)

        val inIds = pkInputs.map(hashed)
        val outIds = pkOutputs.map(o => hashed(o._2))
        inIds.flatMap { in =>
          outIds.map(out => (in, out))
        }
      }
      connection.close()
      payments
    }
  }

  def hashed(bytes: Array[Byte]): Long = ???

}
