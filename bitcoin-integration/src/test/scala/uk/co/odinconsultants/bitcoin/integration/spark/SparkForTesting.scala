package uk.co.odinconsultants.bitcoin.integration.spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkForTesting {

  val master: String = "local[*]"
  val appName: String = "Tests"
  val sparkConf: SparkConf    = new SparkConf().setMaster(master).setAppName(appName)
  val sc: SparkContext        = SparkContext.getOrCreate(sparkConf)

}
