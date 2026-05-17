package state

import org.apache.spark.sql.{DataFrame, SparkSession}

object SessionStateStore {
  def loadSnapshot(spark: SparkSession, basePath: String, snapshotDateKst: String): DataFrame =
    spark.read.parquet(basePath)

  def saveSnapshot(snapshot: DataFrame, basePath: String): Unit =
    snapshot.write.mode("overwrite").parquet(basePath)
}
