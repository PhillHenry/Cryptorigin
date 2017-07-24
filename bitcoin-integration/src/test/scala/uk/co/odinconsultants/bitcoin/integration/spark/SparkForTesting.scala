package uk.co.odinconsultants.bitcoin.integration.spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkForTesting {

  val sparkConf: SparkConf    = new SparkConf().setMaster("local[*]").setAppName("Tests")
  val sc: SparkContext        = SparkContext.getOrCreate(sparkConf)

}
