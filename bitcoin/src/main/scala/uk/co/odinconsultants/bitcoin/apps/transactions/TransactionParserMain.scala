package uk.co.odinconsultants.bitcoin.apps.transactions

import org.apache.spark.SparkContext
import uk.co.odinconsultants.bitcoin.apps.SparkBlockChain.blockChainRdd
import uk.co.odinconsultants.bitcoin.apps.SparkClient
import uk.co.odinconsultants.bitcoin.apps.transactions.TransactionParserConf.{TransactionParserConfig, parse}
import uk.co.odinconsultants.bitcoin.core.Logging
import uk.co.odinconsultants.bitcoin.hbase.HBaseSetup.connection
import uk.co.odinconsultants.bitcoin.parsing.Indexer.{toGraph, transactionsOf}

object TransactionParserMain extends SparkClient with Logging {

  def main(args: Array[String]): Unit = {
    val config = parse(args)
    config match {
      case Some(x) => process(sparkContext, x)
      case None    => error(s"Cannot parse ${args.mkString(", ")}")
    }
  }

  def process(sc: SparkContext, config: TransactionParserConfig): Unit = {
    val rdd       = blockChainRdd(sc, config.inputUrl, sc.hadoopConfiguration)
    val txs       = transactionsOf(rdd)
    val adjacency = toGraph(txs, () => connection())
    adjacency.saveAsObjectFile(config.outputUrl)
  }

}
