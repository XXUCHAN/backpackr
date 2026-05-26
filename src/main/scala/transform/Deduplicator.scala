package transform

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Deduplicator {
  private val DedupKeyColumn = "dedup_key"
  private val NullToken = "__null__"

  def apply(df: DataFrame): DataFrame =
    analyze(df).deduplicated

  def analyze(df: DataFrame): DeduplicationResult = {
    val keyed = withDedupKey(df)
    val duplicateGroups = duplicateGroupsOnly(keyed)

    DeduplicationResult(
      deduplicated = keyed.dropDuplicates(DedupKeyColumn),
      duplicateGroups = duplicateGroups
    )
  }

  private def withDedupKey(df: DataFrame): DataFrame =
    df.withColumn(
      DedupKeyColumn,
      sha2(
        concat_ws(
          "||",
          keyPart(col("user_id")),
          keyPart(unix_millis(col("event_time_utc"))),
          keyPart(col("event_type")),
          keyPart(col("product_id")),
          keyPart(col("category_id")),
          keyPart(col("category_code")),
          keyPart(col("brand")),
          keyPart(col("normalized_price")),
          keyPart(col("raw_user_session"))
        ),
        256
      )
    )

  private def duplicateGroupsOnly(df: DataFrame): DataFrame =
    df
      .groupBy(DedupKeyColumn)
      .count()
      .withColumnRenamed("count", "duplicate_group_size")
      .filter(col("duplicate_group_size") > 1)
      .withColumn("dropped_duplicate_row_count", col("duplicate_group_size") - lit(1L))

  private def keyPart(column: Column): Column =
    coalesce(column.cast("string"), lit(NullToken))
}

final case class DeduplicationResult(deduplicated: DataFrame, duplicateGroups: DataFrame)
