package transform

import org.apache.spark.sql.DataFrame

object Deduplicator {
  def apply(df: DataFrame): DataFrame = df
}
