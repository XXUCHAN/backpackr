package query

import org.apache.spark.sql.{DataFrame, SparkSession}

object WauQueryExecutor {
  def runUserWau(
      spark: SparkSession,
      tableName: String,
      affectedWeekStart: Option[String] = None,
      affectedWeekEnd: Option[String] = None
  ): DataFrame =
    spark.sql(WauQueries.renderUserWauQuery(tableName, affectedWeekStart, affectedWeekEnd))

  def runWeeklyActiveSessions(
      spark: SparkSession,
      tableName: String,
      affectedWeekStart: Option[String] = None,
      affectedWeekEnd: Option[String] = None
  ): DataFrame =
    spark.sql(WauQueries.renderWeeklyActiveSessionsQuery(tableName, affectedWeekStart, affectedWeekEnd))

  def writeResult(df: DataFrame, outputPath: String): Unit =
    df.coalesce(1)
      .write
      .option("partitionOverwriteMode", "dynamic")
      .mode("overwrite")
      .partitionBy("week_start_kst")
      .parquet(outputPath)
}
