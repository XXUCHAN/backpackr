package sessionization

import org.apache.spark.sql.Row
import schema.ActivitySchema
import support.SparkFunSuite
import transform.{ActivityNormalizer, Deduplicator}

class SessionizerSpec extends SparkFunSuite {
  test("sessionizer should keep events within 299 seconds in the same session and split at 300 seconds") {
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row("2019-10-01 00:00:00 UTC", "view", Long.box(1001L), Long.box(2001L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-a"),
          Row("2019-10-01 00:04:59 UTC", "cart", Long.box(1001L), Long.box(2001L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-a"),
          Row("2019-10-01 00:09:59 UTC", "purchase", Long.box(1001L), Long.box(2001L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-a")
        )
      ),
      ActivitySchema.rawSchema
    )

    val sessionized = Sessionizer(Deduplicator(ActivityNormalizer(input, "run-1")))

    val rows = sessionized
      .selectExpr(
        "date_format(event_time_utc, 'yyyy-MM-dd HH:mm:ss') as event_time_utc_str",
        "date_format(session_start_time_utc, 'yyyy-MM-dd HH:mm:ss') as session_start_time_utc_str",
        "session_id"
      )
      .collect()
      .map(row => (row.getString(0), row.getString(1), row.getString(2)))
      .sortBy(_._1)

    assert(rows.length === 3)
    assert(rows(0)._2 === "2019-10-01 00:00:00")
    assert(rows(1)._2 === "2019-10-01 00:00:00")
    assert(rows(2)._2 === "2019-10-01 00:09:59")
    assert(rows(0)._3 === rows(1)._3)
    assert(rows(1)._3 !== rows(2)._3)
  }

  test("sessionizer should split sessions independently for each user") {
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row("2019-10-01 00:00:00 UTC", "view", Long.box(1001L), Long.box(2001L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-a"),
          Row("2019-10-01 00:05:00 UTC", "view", Long.box(1002L), Long.box(2002L), "electronics.audio", "sony", Double.box(11.5d), Long.box(3001L), "session-a"),
          Row("2019-10-01 00:00:00 UTC", "view", Long.box(1001L), Long.box(2001L), "electronics.audio", "sony", Double.box(12.5d), Long.box(4001L), "session-b"),
          Row("2019-10-01 00:04:00 UTC", "cart", Long.box(1002L), Long.box(2002L), "electronics.audio", "sony", Double.box(12.5d), Long.box(4001L), "session-b")
        )
      ),
      ActivitySchema.rawSchema
    )

    val sessionized = Sessionizer(Deduplicator(ActivityNormalizer(input, "run-2")))

    val groupedByUser = sessionized
      .selectExpr(
        "cast(user_id as string) as user_id_str",
        "date_format(session_start_time_utc, 'yyyy-MM-dd HH:mm:ss') as session_start_time_utc_str",
        "session_id"
      )
      .collect()
      .groupBy(_.getString(0))
      .mapValues(rows => rows.map(row => (row.getString(1), row.getString(2))).toSeq)

    assert(groupedByUser("3001").map(_._2).distinct.size === 2)
    assert(groupedByUser("4001").map(_._2).distinct.size === 1)
    assert(groupedByUser("3001").map(_._1).sorted === Seq("2019-10-01 00:00:00", "2019-10-01 00:05:00"))
    assert(groupedByUser("4001").map(_._1).distinct === Seq("2019-10-01 00:00:00"))
  }

  test("sessionizer should add session columns with non-null values") {
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row("2019-10-01 00:00:00 UTC", "view", Long.box(1001L), Long.box(2001L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-a"),
          Row("2019-10-01 00:00:00 UTC", "cart", Long.box(1002L), Long.box(2002L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-a")
        )
      ),
      ActivitySchema.rawSchema
    )

    val sessionized = Sessionizer(Deduplicator(ActivityNormalizer(input, "run-3")))

    assert(sessionized.columns.contains("session_start_time_utc"))
    assert(sessionized.columns.contains("session_start_time_kst"))
    assert(sessionized.columns.contains("session_id"))
    assert(sessionized.filter("session_start_time_utc is null").count() === 0L)
    assert(sessionized.filter("session_start_time_kst is null").count() === 0L)
    assert(sessionized.filter("session_id is null").count() === 0L)
  }

  test("sessionizer should keep the same session across KST midnight when gap is less than 5 minutes") {
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row("2019-10-01 14:58:00 UTC", "view", Long.box(1001L), Long.box(2001L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-a"),
          Row("2019-10-01 15:02:00 UTC", "cart", Long.box(1002L), Long.box(2002L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-a")
        )
      ),
      ActivitySchema.rawSchema
    )

    val sessionized = Sessionizer(Deduplicator(ActivityNormalizer(input, "run-4")))

    val rows = sessionized
      .selectExpr(
        "date_format(event_time_kst, 'yyyy-MM-dd HH:mm:ss') as event_time_kst_str",
        "cast(event_date_kst as string) as event_date_kst_str",
        "date_format(session_start_time_kst, 'yyyy-MM-dd HH:mm:ss') as session_start_time_kst_str",
        "session_id"
      )
      .collect()
      .map(row => (row.getString(0), row.getString(1), row.getString(2), row.getString(3)))
      .sortBy(_._1)

    assert(rows.length === 2)
    assert(rows.map(_._2) === Seq("2019-10-01", "2019-10-02"))
    assert(rows.map(_._3).distinct === Seq("2019-10-01 23:58:00"))
    assert(rows.map(_._4).distinct.length === 1)
  }
}
