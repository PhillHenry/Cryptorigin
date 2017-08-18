package uk.co.odinconsultants.bitcoin.apps.transactions

import org.apache.spark.SparkContext
import uk.co.odinconsultants.bitcoin.apps.SparkBlockChain.blockChainRdd
import uk.co.odinconsultants.bitcoin.apps.SparkClient
import uk.co.odinconsultants.bitcoin.core.Logging

object TransactionParserMain extends SparkClient with Logging {

  def main(args: Array[String]): Unit = {

  }

  def process(sc: SparkContext, url: String): Unit = {
    val rdd     = blockChainRdd(sc, url, sc.hadoopConfiguration)
  }

}
