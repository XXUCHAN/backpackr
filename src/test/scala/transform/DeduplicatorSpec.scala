package transform

import org.apache.spark.sql.Row
import schema.ActivitySchema
import support.SparkFunSuite

class DeduplicatorSpec extends SparkFunSuite {
  test("deduplicator should add dedup_key and remove duplicated events") {
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row("2019-10-01 00:00:00 UTC", "view", Long.box(1001L), Long.box(2001L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-a"),
          Row("2019-10-01 00:00:00 UTC", "view", Long.box(1001L), Long.box(2001L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-b"),
          Row("2019-10-01 00:00:01 UTC", "view", Long.box(1001L), Long.box(2001L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-c"),
          Row("2019-10-01 00:00:00 UTC", "purchase", Long.box(1001L), Long.box(2001L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-d")
        )
      ),
      ActivitySchema.rawSchema
    )

    val normalized = ActivityNormalizer(input, "run-1")
    val result = Deduplicator(normalized)

    assert(result.count() === 3L)
    assert(result.columns.contains("dedup_key"))

    val keys = result.select("dedup_key").collect().map(_.getString(0))
    assert(keys.forall(key => key != null && key.length == 64))
    assert(keys.distinct.length === 3)

    val remainingEventTimes = result
      .selectExpr("date_format(event_time_utc, 'yyyy-MM-dd HH:mm:ss') as event_time_utc_str", "event_type")
      .collect()
      .map(row => (row.getString(0), row.getString(1)))
      .toSet

    assert(remainingEventTimes.contains("2019-10-01 00:00:00" -> "view"))
    assert(remainingEventTimes.contains("2019-10-01 00:00:01" -> "view"))
    assert(remainingEventTimes.contains("2019-10-01 00:00:00" -> "purchase"))
  }
}
