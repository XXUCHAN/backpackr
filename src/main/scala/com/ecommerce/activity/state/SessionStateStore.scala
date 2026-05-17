package com.ecommerce.activity.state

import org.apache.spark.sql.{DataFrame, SparkSession}

object SessionStateStore {
  def loadSnapshot(spark: SparkSession, basePath: String, snapshotDateKst: String): DataFrame = {
    val _ = snapshotDateKst
    spark.read.parquet(basePath)
  }

  def saveSnapshot(snapshot: DataFrame, basePath: String): Unit =
    snapshot.write.mode("overwrite").parquet(basePath)
}
