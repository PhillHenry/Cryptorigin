package uk.co.odinconsultants.bitcoin.apps

import org.apache.spark.{SparkConf, SparkContext}

trait SparkClient {

  def sparkContext: SparkContext = {
    val sparkConfig = new SparkConf()
    sparkConfig.setAppName(this.getClass.getSimpleName)
    new SparkContext(sparkConfig)
  }

}
