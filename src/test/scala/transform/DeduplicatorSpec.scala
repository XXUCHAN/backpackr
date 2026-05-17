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

    assert(result.count() === 4L)
    assert(result.columns.contains("dedup_key"))

    val keys = result.select("dedup_key").collect().map(_.getString(0))
    assert(keys.forall(key => key != null && key.length == 64))
    assert(keys.distinct.length === 4)

    val remainingRows = result
      .selectExpr(
        "date_format(event_time_utc, 'yyyy-MM-dd HH:mm:ss') as event_time_utc_str",
        "event_type",
        "raw_user_session"
      )
      .collect()
      .map(row => (row.getString(0), row.getString(1), row.getString(2)))
      .toSet

    assert(remainingRows.contains(("2019-10-01 00:00:00", "view", "session-a")))
    assert(remainingRows.contains(("2019-10-01 00:00:00", "view", "session-b")))
    assert(remainingRows.contains(("2019-10-01 00:00:01", "view", "session-c")))
    assert(remainingRows.contains(("2019-10-01 00:00:00", "purchase", "session-d")))
  }

  test("deduplicator should expose duplicate groups for inspection") {
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row("2019-10-01 00:00:00 UTC", "view", Long.box(1001L), Long.box(2001L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-a"),
          Row("2019-10-01 00:00:00 UTC", "view", Long.box(1001L), Long.box(2001L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-a"),
          Row("2019-10-01 00:00:01 UTC", "view", Long.box(1001L), Long.box(2001L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-c")
        )
      ),
      ActivitySchema.rawSchema
    )

    val normalized = ActivityNormalizer(input, "run-2")
    val result = Deduplicator.analyze(normalized)

    assert(result.deduplicated.count() === 2L)
    assert(result.duplicates.count() === 2L)
    assert(result.duplicates.columns.contains("duplicate_group_size"))
    assert(result.duplicates.columns.contains("duplicate_rank"))
    assert(result.duplicates.columns.contains("dedup_retained"))

    val duplicateRows = result.duplicates
      .select("raw_user_session", "duplicate_group_size", "duplicate_rank", "dedup_retained")
      .collect()
      .map(row => (row.getString(0), row.getLong(1), row.getInt(2), row.getBoolean(3)))
      .sortBy(_._3)

    assert(duplicateRows.length === 2)
    assert(duplicateRows.forall(_._2 == 2L))
    assert(duplicateRows.map(_._3).sameElements(Array(1, 2)))
    assert(duplicateRows.count(_._4) === 1)
    assert(duplicateRows.map(_._1).toSet === Set("session-a"))
  }

  test("deduplicator should keep rows that differ only by raw_user_session") {
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row("2019-10-01 00:00:00 UTC", "view", Long.box(1001L), Long.box(2001L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-a"),
          Row("2019-10-01 00:00:00 UTC", "view", Long.box(1001L), Long.box(2001L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-b")
        )
      ),
      ActivitySchema.rawSchema
    )

    val normalized = ActivityNormalizer(input, "run-3")
    val result = Deduplicator.analyze(normalized)

    assert(result.deduplicated.count() === 2L)
    assert(result.duplicates.count() === 0L)
  }
}
