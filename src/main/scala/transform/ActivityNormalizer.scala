package transform

import schema.ActivitySchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

object ActivityNormalizer {
  private val PricePrecision = DecimalType(18, 2)
  private val KstZoneId = "Asia/Seoul"
  private val WeekStartDay = "MON"

  def apply(df: DataFrame, runId: String): DataFrame =
    df.withColumn("event_time_utc", to_timestamp(col("event_time"), ActivitySchema.EventTimePattern))
      .withColumn("event_time_kst", from_utc_timestamp(col("event_time_utc"), KstZoneId))
      .withColumn("event_date_kst", to_date(col("event_time_kst")))
      .withColumn("week_start_kst", date_sub(next_day(col("event_date_kst"), WeekStartDay), 7))
      .withColumnRenamed("user_session", "raw_user_session")
      .withColumn("normalized_price", col("price").cast(PricePrecision))
      .withColumn("ingested_at", current_timestamp())
      .withColumn("run_id", lit(runId))
}
