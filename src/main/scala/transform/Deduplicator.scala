package transform

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Deduplicator {
  private val DedupKeyColumn = "dedup_key"
  private val NullToken = "__null__"

  def apply(df: DataFrame): DataFrame =
    analyze(df).deduplicated

  def analyze(df: DataFrame): DeduplicationResult = {
    val keyed = withDedupKey(df)
    val duplicatesOnly = duplicateRows(keyed)

    DeduplicationResult(
      deduplicated = keyed.dropDuplicates(DedupKeyColumn),
      duplicates = duplicatesOnly
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

  private def duplicateRows(df: DataFrame): DataFrame = {
    val duplicateCounts = df
      .groupBy(DedupKeyColumn)
      .count()
      .withColumnRenamed("count", "duplicate_group_size")
      .filter(col("duplicate_group_size") > 1)

    val orderWindow = Window
      .partitionBy(DedupKeyColumn)
      .orderBy(
        col("event_time_utc"),
        col("event_type"),
        col("product_id"),
        col("category_id"),
        col("category_code"),
        col("brand"),
        col("normalized_price"),
        col("user_id"),
        col("raw_user_session")
      )

    df.join(duplicateCounts, Seq(DedupKeyColumn), "inner")
      .withColumn("duplicate_rank", row_number().over(orderWindow))
      .withColumn("dedup_retained", col("duplicate_rank") === lit(1))
      .orderBy(col(DedupKeyColumn), col("duplicate_rank"))
  }

  private def keyPart(column: Column): Column =
    coalesce(column.cast("string"), lit(NullToken))
}

final case class DeduplicationResult(deduplicated: DataFrame, duplicates: DataFrame)
