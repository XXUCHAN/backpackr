package logging

import model.BatchRunStatus
import org.apache.spark.sql.SparkSession

object BatchRunLogger {
  def logStatus(
      spark: SparkSession,
      runId: String,
      targetDate: String,
      status: BatchRunStatus,
      message: Option[String] = None
  ): Unit = ()
}
