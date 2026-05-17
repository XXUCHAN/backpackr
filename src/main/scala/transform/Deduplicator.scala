package transform

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Deduplicator {
  private val DedupKeyColumn = "dedup_key"
  private val NullToken = "__null__"

  def apply(df: DataFrame): DataFrame =
    df.withColumn(
        DedupKeyColumn,
        sha2(
          concat_ws(
            "||",
            keyPart(col("user_id")),
            keyPart(unix_millis(col("event_time_utc"))),
            keyPart(col("event_type")),
            keyPart(col("product_id")),
            keyPart(col("normalized_price"))
          ),
          256
        )
      )
      .dropDuplicates(DedupKeyColumn)

  private def keyPart(column: Column): Column =
    coalesce(column.cast("string"), lit(NullToken))
}
