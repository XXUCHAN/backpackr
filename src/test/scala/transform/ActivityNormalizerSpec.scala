package transform

import schema.ActivitySchema
import support.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, date_format}

class ActivityNormalizerSpec extends SparkFunSuite {
  test("normalizer should create UTC/KST time columns and normalized metadata") {
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(
            "2019-10-01 00:00:00 UTC",
            "view",
            Long.box(1001L),
            Long.box(2002L),
            "electronics.smartphone",
            "apple",
            Double.box(10.5d),
            Long.box(3003L),
            "raw-session-1"
          )
        )
      ),
      ActivitySchema.rawSchema
    )

    val normalized = ActivityNormalizer(input, "run-1")
    val row = normalized
      .select(
        date_format(col("event_time_utc"), "yyyy-MM-dd HH:mm:ss").as("event_time_utc_str"),
        date_format(col("event_time_kst"), "yyyy-MM-dd HH:mm:ss").as("event_time_kst_str"),
        col("event_date_kst").cast("string").as("event_date_kst_str"),
        col("normalized_price"),
        col("raw_user_session"),
        col("run_id"),
        col("ingested_at")
      )
      .head()

    assert(row.getAs[String]("event_time_utc_str") === "2019-10-01 00:00:00")
    assert(row.getAs[String]("event_time_kst_str") === "2019-10-01 09:00:00")
    assert(row.getAs[String]("event_date_kst_str") === "2019-10-01")
    assert(row.getAs[java.math.BigDecimal]("normalized_price").toPlainString === "10.50")
    assert(row.getAs[String]("raw_user_session") === "raw-session-1")
    assert(row.getAs[String]("run_id") === "run-1")
    assert(row.getAs[java.sql.Timestamp]("ingested_at") != null)
    assert(!normalized.columns.contains("user_session"))
  }
}
