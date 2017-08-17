package uk.co.odinconsultants.bitcoin.apps

import org.apache.spark.{SparkConf, SparkContext}
import uk.co.odinconsultants.bitcoin.apps.SparkBlockChain.blockChainRdd
import uk.co.odinconsultants.bitcoin.core.Logging
import uk.co.odinconsultants.bitcoin.hbase.HBaseSetup
import uk.co.odinconsultants.bitcoin.hbase.HBaseSetup.connection
import uk.co.odinconsultants.bitcoin.parsing.Indexer.{index, write}

object TransactionKeyMappingsMain extends Logging {

  import TransactionKeyMappingsConf._

  def main(args: Array[String]): Unit = {
    parse(args) match {
      case Some(c)  => process(c)
      case None     => error(s"Cannot parse ${args.mkString(", ")}")
    }
  }

  def process(config: KeyMappingConfig): Unit = {
    if (config.refresh) {
      val conn = HBaseSetup.connection()
      HBaseSetup.createAddressesTable(conn.getAdmin)
      conn.close()
    }
    indexTransactions(sparkContext, config)
  }

  private def sparkContext = {
    val sparkConfig = new SparkConf()
    sparkConfig.setAppName(this.getClass.getSimpleName)
    new SparkContext(sparkConfig)
  }

  def indexTransactions(sc: SparkContext, config: KeyMappingConfig): Unit = {
    val rdd = blockChainRdd(sc, config.url, sc.hadoopConfiguration)
    val outputs = index(rdd)
    write(outputs, () => connection())
  }

}
