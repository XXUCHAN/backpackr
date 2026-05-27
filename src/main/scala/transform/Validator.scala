package transform

import schema.ActivitySchema
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

final case class ValidationResult(valid: DataFrame, invalid: DataFrame)

object Validator {
  def apply(df: DataFrame, targetDate: String): ValidationResult =
    apply(df, coalesce(col("event_date_kst").cast("string"), lit(targetDate)))

  def apply(df: DataFrame, targetDate: Column): ValidationResult = {
    val invalidEventTime = col("event_time_utc").isNull
    val nullUserId = col("user_id").isNull
    val nullEventType = col("event_type").isNull
    val nullProductId = col("product_id").isNull
    val negativePrice = coalesce(col("price") < lit(0.0d), lit(false))

    val invalidCondition =
      invalidEventTime || nullUserId || nullEventType || nullProductId || negativePrice

    val rejectReason = concat_ws(
      "|",
      when(invalidEventTime, lit("INVALID_EVENT_TIME")),
      when(nullUserId, lit("NULL_USER_ID")),
      when(nullEventType, lit("NULL_EVENT_TYPE")),
      when(nullProductId, lit("NULL_PRODUCT_ID")),
      when(negativePrice, lit("NEGATIVE_PRICE"))
    )

    val rawPayload = to_json(struct(ActivitySchema.rawPayloadFieldNames.map(col): _*))

    val invalid = df
      .filter(invalidCondition)
      .withColumn("reject_reason", rejectReason)
      .withColumn("target_date", targetDate)
      .withColumn("raw_payload", rawPayload)

    val valid = df.filter(not(invalidCondition))

    ValidationResult(valid = valid, invalid = invalid)
  }
}
