package query

import org.apache.spark.sql.functions.to_timestamp
import support.SparkFunSuite

class WauQueryExecutorSpec extends SparkFunSuite {
  test("wau query executor should run user and session weekly aggregations on a table") {
    val sparkSession = spark
    import sparkSession.implicits._

    val tableName = "activity_events_wau_test"

    val df = Seq(
      (1L, "s1", "2019-10-01", "2019-10-01 10:00:00"),
      (1L, "s1", "2019-10-01", "2019-10-01 10:01:00"),
      (2L, "s2", "2019-10-02", "2019-10-02 12:00:00"),
      (3L, "s3", "2019-10-08", "2019-10-08 09:00:00")
    ).toDF("user_id", "session_id", "event_date_kst", "session_start_time_kst_raw")
      .withColumn("event_date_kst", $"event_date_kst".cast("date"))
      .withColumn("session_start_time_kst", to_timestamp($"session_start_time_kst_raw"))
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
}
