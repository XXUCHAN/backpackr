package query

import org.apache.spark.sql.{DataFrame, SparkSession}

object WauQueryExecutor {
  def runUserWau(spark: SparkSession, tableName: String): DataFrame =
    spark.sql(WauQueries.renderUserWauQuery(tableName))

  def runWeeklyActiveSessions(spark: SparkSession, tableName: String): DataFrame =
    spark.sql(WauQueries.renderWeeklyActiveSessionsQuery(tableName))

  def writeResult(df: DataFrame, outputPath: String): Unit =
    df.coalesce(1)
      .write
      .mode("overwrite")
      .parquet(outputPath)
}
