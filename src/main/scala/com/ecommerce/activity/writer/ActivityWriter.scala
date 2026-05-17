package com.ecommerce.activity.writer

import org.apache.spark.sql.DataFrame

object ActivityWriter {
  def writeToStaging(df: DataFrame, stagingPath: String): Unit =
    df.write.mode("overwrite").parquet(stagingPath)
}
