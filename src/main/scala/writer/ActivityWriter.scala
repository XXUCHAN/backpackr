package writer

import org.apache.spark.sql.DataFrame

object ActivityWriter {
  def writeToStaging(df: DataFrame, stagingPath: String): Unit =
    df.write
      .mode("overwrite")
      .partitionBy("event_date_kst")
      .parquet(stagingPath)
}
