package uk.co.odinconsultants.bitcoin.parsing

import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.bitcoinj.params.MainNetParams
import org.zuinnote.hadoop.bitcoin.format.common.{BitcoinBlock, BitcoinTransaction}
import uk.co.odinconsultants.bitcoin.hbase.HBaseSetup.{familyName, tableName}
import uk.co.odinconsultants.bitcoin.hbase.{HBaseMetaRetrieval, HBaseMetaStore, HBaseSetup}
import uk.co.odinconsultants.bitcoin.parsing.DomainOps._
import uk.co.odinconsultants.bitcoin.parsing.MetaStore.Payload
import util.hash.MurmurHash3

import scala.collection.JavaConversions._

object Indexer {

  type PubKey         = Array[Byte]
  type BackReference  = (Array[Byte], Long)

  val networkParams: MainNetParams = MainNetParams.get()

  def index(rdd: RDD[(BytesWritable, BitcoinBlock)]): RDD[Payload]
    = rdd.flatMap{ case(_, block) => toTransactions(block) }.flatMap(toBackReferenceAddressTuples)

  def transactionsOf(rdd: RDD[(BytesWritable, BitcoinBlock)]): RDD[BitcoinTransaction]
    = rdd.flatMap(_._2.getTransactions)

  val batchSize = 100

  def write(rdd: RDD[Payload], connectionFactory: () => Connection): Unit = {
    rdd.foreachPartition { iter =>
      val connection  = connectionFactory()
      val table       = connection.getTable(tableName)
      val metaStore   = new HBaseMetaStore(table, familyName)

      iter.grouped(batchSize).foreach { metaIter =>
        val payloads = metaIter.toList
//        debug(s"Storing: ${payloads.map(x => (new String(Hex.encodeHex(x._1._1)), x._1._2)).mkString(", ")}")
        metaStore(payloads)
      }

      connection.close()
    }
  }

  def toGraph(rdd: RDD[BitcoinTransaction], connectionFactory: () => Connection): RDD[(Long, Long)] = {
    rdd.mapPartitions { txs =>

      txs.grouped(10000).flatMap { batch =>
        val connection  = connectionFactory()
        val table       = connection.getTable(tableName)
        val metaStore   = new HBaseMetaRetrieval(table, familyName)

        val batched     = batch.flatMap { cartesianProductOfIO(_, metaStore) }

        connection.close()
        batched
      }

    }
  }

  def catalogueAddresses(sc: SparkSession): DataFrame = {
    val sqlContext = sc.sqlContext
    import sqlContext.implicits._
    val cat = s"""{
                  |"table":{"namespace":"default", "name":"${HBaseSetup.metaTable}", "tableCoder":"PrimitiveType"},
                  |"rowkey":"key",
                  |"columns":{
                  |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                  |"col1":{"cf":"${HBaseSetup.familyName}", "col":"${HBaseSetup.qualifier}", "type":"binary"}
                  |}
                  |}""".stripMargin
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  def cartesianProductOfIO(tx: BitcoinTransaction, metaStore: HBaseMetaRetrieval): Seq[(Long, Long)] = {
    val inputs    = tx.getListOfInputs
    val backRefs  = inputs.map { i =>
      (i.getPrevTransactionHash, i.getPreviousTxOutIndex)
    }
    val pkInputs  = metaStore(backRefs.toList)
//    debug(s"Looking up: ${backRefs.map(x => (new String(Hex.encodeHex(x._1)), x._2)).mkString(", ")} got ${pkInputs.mkString(", ")}")

    val pkOutputs = toBackReferenceAddressTuples(tx)

    val inIds     = pkInputs.filterNot(_ == null).map(hashed)
    val outIds    = pkOutputs.map(o => hashed(o._2))
    inIds.flatMap { in =>
      outIds.map(out => (in, out))
    }
  }

  def hashed(bytes: Array[Byte]): Long = {
    val pair = new MurmurHash3.LongPair
    MurmurHash3.murmurhash3_x64_128(bytes, 0, bytes.length, 0, pair)
    pair.val1
  }

}
