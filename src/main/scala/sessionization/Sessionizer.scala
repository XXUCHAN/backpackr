package sessionization

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Sessionizer {
  private val SessionGapSeconds = 300L
  private val NullToken = "__null__"
  private val KstZoneId = "Asia/Seoul"

  def apply(df: DataFrame): DataFrame = {
    val orderingWindow = Window
      .partitionBy("user_id")
      .orderBy(
        col("event_time_utc"),
        col("product_id"),
        col("event_type"),
        col("dedup_key")
      )

    val runningWindow = orderingWindow.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val withPreviousEventTime =
      df.withColumn("previous_event_time_utc", lag(col("event_time_utc"), 1).over(orderingWindow))

    val withSessionFlags = withPreviousEventTime
      .withColumn(
        "is_new_session",
        when(col("previous_event_time_utc").isNull, lit(1))
          .when(
            unix_timestamp(col("event_time_utc")) - unix_timestamp(col("previous_event_time_utc")) >= lit(SessionGapSeconds),
            lit(1)
          )
          .otherwise(lit(0))
      )
      .withColumn(
        "session_start_time_utc",
        last(
          when(col("is_new_session") === lit(1), col("event_time_utc")),
          ignoreNulls = true
        ).over(runningWindow)
      )

    withSessionFlags
      .withColumn("session_start_time_kst", from_utc_timestamp(col("session_start_time_utc"), KstZoneId))
      .withColumn(
        "session_id",
        sha2(
          concat_ws(
            "||",
            coalesce(col("user_id").cast("string"), lit(NullToken)),
            coalesce(unix_millis(col("session_start_time_utc")).cast("string"), lit(NullToken))
          ),
          256
        )
      )
      .drop("previous_event_time_utc", "is_new_session")
  }
}
