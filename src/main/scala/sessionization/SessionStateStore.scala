package sessionization

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import support.PathBuilder

import java.nio.file.{Files, Paths}

object SessionStateStore {
  def loadSnapshot(spark: SparkSession, basePath: String, snapshotDateKst: String): Option[DataFrame] = {
    val snapshotPath = PathBuilder.sessionSnapshotPath(basePath, snapshotDateKst)

    if (Files.exists(Paths.get(snapshotPath))) {
      Some(spark.read.parquet(snapshotPath))
    } else {
      None
    }
  }

  def saveSnapshot(snapshot: DataFrame, basePath: String, snapshotDateKst: String): String = {
    val snapshotPath = PathBuilder.sessionSnapshotPath(basePath, snapshotDateKst)

    snapshot.write.mode("overwrite").parquet(snapshotPath)
    snapshotPath
  }

  def buildSnapshot(sessionized: DataFrame, snapshotDateKst: String, runId: String): DataFrame = {
    val lastEventWindow = Window
      .partitionBy("user_id")
      .orderBy(
        col("event_time_utc").desc,
        col("product_id").desc,
        col("event_type").desc,
        col("dedup_key").desc
      )

    sessionized
      .withColumn("snapshot_rank", row_number().over(lastEventWindow))
      .filter(col("snapshot_rank") === lit(1))
      .select(
        lit(snapshotDateKst).as("snapshot_date_kst"),
        col("user_id"),
        col("session_id").as("last_session_id"),
        col("session_start_time_utc").as("last_session_start_time_utc"),
        col("event_time_utc").as("last_event_time_utc"),
        col("event_time_kst").as("last_event_time_kst"),
        current_timestamp().as("updated_at"),
        lit(runId).as("run_id")
      )
  }
}
