package com.ecommerce.activity.support

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {
  def create(appName: String): SparkSession =
    SparkSession
      .builder()
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()
}
