package transform

import schema.ActivitySchema
import support.SparkFunSuite
import org.apache.spark.sql.Row

class ValidatorSpec extends SparkFunSuite {
  test("validator should split valid rows and invalid rows with reject reasons") {
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row("2019-10-01 00:00:00 UTC", "view", Long.box(1L), Long.box(10L), "a", "brand-a", Double.box(9.99d), Long.box(101L), "session-1"),
          Row("bad-time", "view", Long.box(2L), Long.box(10L), "b", "brand-b", Double.box(19.99d), Long.box(102L), "session-2"),
          Row("2019-10-01 00:01:00 UTC", "cart", Long.box(3L), Long.box(10L), "c", "brand-c", Double.box(29.99d), null, "session-3"),
          Row("2019-10-01 00:02:00 UTC", null, Long.box(4L), Long.box(10L), "d", "brand-d", Double.box(39.99d), Long.box(104L), "session-4"),
          Row("2019-10-01 00:03:00 UTC", "purchase", null, Long.box(10L), "e", "brand-e", Double.box(49.99d), Long.box(105L), "session-5"),
          Row("2019-10-01 00:04:00 UTC", "view", Long.box(6L), Long.box(10L), "f", "brand-f", Double.box(-1.0d), Long.box(106L), "session-6")
        )
      ),
      ActivitySchema.rawSchema
    )

    val normalized = ActivityNormalizer(input, "run-1")
    val result = Validator(normalized, targetDate = "2019-10-01")

    assert(result.valid.count() === 1L)
    assert(result.invalid.count() === 5L)

    val invalidBySession = result.invalid.collect().map(row => row.getAs[String]("raw_user_session") -> row).toMap

    assert(invalidBySession("session-2").getAs[String]("reject_reason") === "INVALID_EVENT_TIME")
    assert(invalidBySession("session-3").getAs[String]("reject_reason") === "NULL_USER_ID")
    assert(invalidBySession("session-4").getAs[String]("reject_reason") === "NULL_EVENT_TYPE")
    assert(invalidBySession("session-5").getAs[String]("reject_reason") === "NULL_PRODUCT_ID")
    assert(invalidBySession("session-6").getAs[String]("reject_reason") === "NEGATIVE_PRICE")

    val sampleDlqRow = invalidBySession("session-2")
    assert(sampleDlqRow.getAs[String]("target_date") === "2019-10-01")
    assert(sampleDlqRow.getAs[String]("run_id") === "run-1")
    assert(sampleDlqRow.getAs[String]("raw_payload").contains("\"event_time\":\"bad-time\""))
    assert(sampleDlqRow.getAs[String]("raw_payload").contains("\"raw_user_session\":\"session-2\""))
  }
}
