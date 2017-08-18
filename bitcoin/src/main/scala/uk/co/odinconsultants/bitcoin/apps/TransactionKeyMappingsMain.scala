package uk.co.odinconsultants.bitcoin.apps

import org.apache.spark.{SparkConf, SparkContext}
import uk.co.odinconsultants.bitcoin.apps.SparkBlockChain.blockChainRdd
import uk.co.odinconsultants.bitcoin.core.Logging
import uk.co.odinconsultants.bitcoin.hbase.HBaseSetup._
import uk.co.odinconsultants.bitcoin.parsing.Indexer.{index, write}

/**
  * "you are going to have to build a hash map for each of
  * the transactions, since each input refers to the transaction hash of the previous output."
  *
  * see http://codesuppository.blogspot.co.at/2014/01/how-to-parse-bitcoin-blockchain.html
  *
  * Run with something like:

nohup ~/spark/bin/spark-submit --class uk.co.odinconsultants.bitcoin.apps.TransactionKeyMappingsMain \
--master yarn --deploy-mode client --driver-memory 2g  --executor-memory 2g  --executor-cores 2 --num-executors 12  \
~/bitcoin-1.0-SNAPSHOT-jar-with-dependencies.jar -u hdfs:/blocks/\*.dat -r > submit.log &

  (but without the slash in blocks\*.dat. That's just there to keep ScalaDoc happy.)
  */
object TransactionKeyMappingsMain extends SparkClient with Logging {

  import TransactionKeyMappingsConf._

  def main(args: Array[String]): Unit = {
    parse(args) match {
      case Some(c)  => process(c)
      case None     => error(s"Cannot parse ${args.mkString(", ")}")
    }
  }

  def process(config: KeyMappingConfig): Unit = {
    val conn  = connection()
    prepareMetaTable(config.refresh, conn)
    conn.close()
    indexTransactions(sparkContext, config)
  }

  def indexTransactions(sc: SparkContext, config: KeyMappingConfig): Unit = {
    val rdd     = blockChainRdd(sc, config.url, sc.hadoopConfiguration)
    val outputs = index(rdd)
    write(outputs, () => connection())
  }

}
