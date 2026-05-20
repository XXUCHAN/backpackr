package query

import org.apache.spark.sql.functions.{to_date, to_timestamp}
import support.SparkFunSuite
import java.nio.file.Files

class WauQueryExecutorSpec extends SparkFunSuite {
  test("wau query executor should run user and session weekly aggregations on a table") {
    val sparkSession = spark
    import sparkSession.implicits._

    val tableName = "activity_events_wau_test"

    val df = Seq(
      (1L, "s1", "2019-10-01", "2019-09-30", "2019-10-01 10:00:00", "2019-09-30"),
      (1L, "s1", "2019-10-01", "2019-09-30", "2019-10-01 10:01:00", "2019-09-30"),
      (2L, "s2", "2019-10-02", "2019-09-30", "2019-10-02 12:00:00", "2019-09-30"),
      (3L, "s3", "2019-10-08", "2019-10-07", "2019-10-08 09:00:00", "2019-10-07")
    ).toDF(
      "user_id",
      "session_id",
      "event_date_kst",
      "week_start_kst",
      "session_start_time_kst_raw",
      "session_start_week_kst"
    )
      .withColumn("event_date_kst", $"event_date_kst".cast("date"))
      .withColumn("week_start_kst", to_date($"week_start_kst"))
      .withColumn("session_start_time_kst", to_timestamp($"session_start_time_kst_raw"))
      .withColumn("session_start_week_kst", to_date($"session_start_week_kst"))
      .drop("session_start_time_kst_raw")

    df.createOrReplaceTempView(tableName)

    val userWau = WauQueryExecutor.runUserWau(sparkSession, tableName)
    val weeklySessions = WauQueryExecutor.runWeeklyActiveSessions(sparkSession, tableName)

    val userResults = userWau.collect().map(row => row.getDate(0).toString -> row.getLong(1)).toMap
    val sessionResults = weeklySessions.collect().map(row => row.getDate(0).toString -> row.getLong(1)).toMap

    assert(userResults("2019-09-30") === 2L)
    assert(userResults("2019-10-07") === 1L)
    assert(sessionResults("2019-09-30") === 2L)
    assert(sessionResults("2019-10-07") === 1L)
  }

  test("wau query executor should only scan affected weeks when a week range is provided") {
    val sparkSession = spark
    import sparkSession.implicits._

    val tableName = "activity_events_wau_filter_test"

    val df = Seq(
      (1L, "s1", "2019-09-30", "2019-09-30"),
      (2L, "s2", "2019-10-07", "2019-10-07"),
      (3L, "s3", "2019-10-14", "2019-10-14")
    ).toDF("user_id", "session_id", "week_start_kst", "session_start_week_kst")
      .withColumn("week_start_kst", to_date($"week_start_kst"))
      .withColumn("session_start_week_kst", to_date($"session_start_week_kst"))

    df.createOrReplaceTempView(tableName)

    val userWau = WauQueryExecutor.runUserWau(
      sparkSession,
      tableName,
      affectedWeekStart = Some("2019-10-07"),
      affectedWeekEnd = Some("2019-10-14")
    )
    val weeklySessions = WauQueryExecutor.runWeeklyActiveSessions(
      sparkSession,
      tableName,
      affectedWeekStart = Some("2019-10-07"),
      affectedWeekEnd = Some("2019-10-14")
    )

    val userWeeks = userWau.collect().map(_.getDate(0).toString).toSeq
    val sessionWeeks = weeklySessions.collect().map(_.getDate(0).toString).toSeq

    assert(userWeeks === Seq("2019-10-07", "2019-10-14"))
    assert(sessionWeeks === Seq("2019-10-07", "2019-10-14"))
  }

  test("wau result writer should overwrite only affected week partitions") {
    val sparkSession = spark
    import sparkSession.implicits._

    val outputDir = Files.createTempDirectory("wau-writer-partition-overwrite-")

    try {
      val initial = Seq(
        ("2019-09-30", 10L),
        ("2019-10-07", 20L)
      ).toDF("week_start_kst", "wau_users")
        .withColumn("week_start_kst", to_date($"week_start_kst"))

      WauQueryExecutor.writeResult(initial, outputDir.toString)

      val updated = Seq(
        ("2019-10-07", 99L)
      ).toDF("week_start_kst", "wau_users")
        .withColumn("week_start_kst", to_date($"week_start_kst"))

      WauQueryExecutor.writeResult(updated, outputDir.toString)

      val results = sparkSession.read.parquet(outputDir.toString).collect()
        .map(row => row.getDate(1).toString -> row.getLong(0)).toMap

      assert(results("2019-09-30") === 10L)
      assert(results("2019-10-07") === 99L)
      assert(results.size === 2)
    } finally {
      val iterator = Files.walk(outputDir).sorted(java.util.Comparator.reverseOrder()).iterator()
      while (iterator.hasNext) {
        Files.deleteIfExists(iterator.next())
      }
    }
  }
}
