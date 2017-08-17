package uk.co.odinconsultants.bitcoin.apps

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinBlock
import org.zuinnote.hadoop.bitcoin.format.mapreduce.BitcoinBlockFileInputFormat

object SparkBlockChain {

  def blockChainRdd(sc:     SparkContext,
                    url:    String,
                    hadoop: Configuration): RDD[(BytesWritable, BitcoinBlock)] =
    sc.newAPIHadoopFile(url, classOf[BitcoinBlockFileInputFormat], classOf[BytesWritable], classOf[BitcoinBlock], hadoop)

}
