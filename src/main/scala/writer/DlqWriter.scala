package writer

import org.apache.spark.sql.DataFrame

object DlqWriter {
  def write(df: DataFrame, dlqPath: String): Unit =
    df.write
      .mode("overwrite")
      .partitionBy("target_date")
      .parquet(dlqPath)
}
