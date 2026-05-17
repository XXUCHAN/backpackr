package sessionization

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Sessionizer {
  private val SessionGapSeconds = 300L
  private val NullToken = "__null__"
  private val KstZoneId = "Asia/Seoul"

  def apply(df: DataFrame): DataFrame =
    apply(df, None)

  def apply(df: DataFrame, previousSnapshot: Option[DataFrame]): DataFrame = {
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
        "local_session_start_time_utc",
        last(
          when(col("is_new_session") === lit(1), col("event_time_utc")),
          ignoreNulls = true
        ).over(runningWindow)
      )
      .withColumn("local_session_seq", sum(col("is_new_session")).over(runningWindow))

    val withFinalSessionStartTime = previousSnapshot match {
      case Some(snapshot) => applyPreviousSnapshotCarryOver(withSessionFlags, snapshot)
      case None =>
        withSessionFlags.withColumn("session_start_time_utc", col("local_session_start_time_utc"))
    }

    withFinalSessionStartTime
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
      .drop("previous_event_time_utc", "is_new_session", "local_session_start_time_utc", "local_session_seq")
  }

  private def applyPreviousSnapshotCarryOver(sessionized: DataFrame, snapshot: DataFrame): DataFrame = {
    val firstEventPerUser = sessionized
      .groupBy("user_id")
      .agg(min(col("event_time_utc")).as("first_event_time_utc"))

    val previousState = snapshot.select(
      col("user_id").as("snapshot_user_id"),
      col("last_session_start_time_utc"),
      col("last_event_time_utc")
    )

    val carryOverInfo = firstEventPerUser
      .join(previousState, firstEventPerUser("user_id") === previousState("snapshot_user_id"), "left")
      .withColumn(
        "gap_from_previous_seconds",
        unix_timestamp(col("first_event_time_utc")) - unix_timestamp(col("last_event_time_utc"))
      )
      .withColumn(
        "carry_over_from_previous",
        col("last_event_time_utc").isNotNull &&
          col("gap_from_previous_seconds") >= lit(0L) &&
          col("gap_from_previous_seconds") < lit(SessionGapSeconds)
      )
      .select(
        firstEventPerUser("user_id"),
        col("carry_over_from_previous"),
        col("last_session_start_time_utc")
      )

    sessionized
      .join(carryOverInfo, Seq("user_id"), "left")
      .withColumn(
        "session_start_time_utc",
        when(
          col("local_session_seq") === lit(1) && coalesce(col("carry_over_from_previous"), lit(false)),
          col("last_session_start_time_utc")
        ).otherwise(col("local_session_start_time_utc"))
      )
      .drop("carry_over_from_previous", "last_session_start_time_utc")
  }
}
