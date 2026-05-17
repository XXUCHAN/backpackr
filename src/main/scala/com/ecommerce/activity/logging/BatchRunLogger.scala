package com.ecommerce.activity.logging

import com.ecommerce.activity.model.BatchRunStatus
import org.apache.spark.sql.SparkSession

object BatchRunLogger {
  def logStatus(
      spark: SparkSession,
      runId: String,
      targetDate: String,
      status: BatchRunStatus,
      message: Option[String] = None
  ): Unit = {
    val _ = spark
    val _ = runId
    val _ = targetDate
    val _ = status
    val _ = message
    ()
  }
}
