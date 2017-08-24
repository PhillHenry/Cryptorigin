package org.apache.spark

import org.apache.spark.sql.SparkSession

object SparkConfigUtil {

  def conf(ss: SparkSession, k: String, v: String): Unit = {
    ss.sqlContext.sparkContext.conf.set(k, v)
  }

}
