package sessionization

import org.apache.spark.sql.Row
import schema.ActivitySchema
import support.SparkFunSuite
import transform.{ActivityNormalizer, Deduplicator}

import java.nio.file.Files

class SessionStateStoreSpec extends SparkFunSuite {
  test("session state store should build the latest session snapshot per user") {
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row("2019-10-01 00:00:00 UTC", "view", Long.box(1001L), Long.box(2001L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-a"),
          Row("2019-10-01 00:04:00 UTC", "cart", Long.box(1002L), Long.box(2002L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-a"),
          Row("2019-10-01 00:10:00 UTC", "purchase", Long.box(1003L), Long.box(2003L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-a"),
          Row("2019-10-01 00:01:00 UTC", "view", Long.box(1001L), Long.box(2001L), "electronics.audio", "sony", Double.box(12.5d), Long.box(4001L), "session-b")
        )
      ),
      ActivitySchema.rawSchema
    )

    val sessionized = Sessionizer(Deduplicator(ActivityNormalizer(input, "run-1")))
    val snapshot = SessionStateStore.buildSnapshot(sessionized, "2019-10-01", "run-1")

    assert(snapshot.count() === 2L)

    val user3001 = snapshot
      .filter("user_id = 3001")
      .selectExpr(
        "last_session_id",
        "date_format(last_session_start_time_utc, 'yyyy-MM-dd HH:mm:ss') as session_start_utc_str",
        "date_format(last_event_time_utc, 'yyyy-MM-dd HH:mm:ss') as last_event_utc_str"
      )
      .head()

    assert(user3001.getString(0) != null)
    assert(user3001.getString(1) === "2019-10-01 00:10:00")
    assert(user3001.getString(2) === "2019-10-01 00:10:00")
  }

  test("session state store should save and reload snapshot by snapshot date path") {
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row("2019-10-01 00:00:00 UTC", "view", Long.box(1001L), Long.box(2001L), "electronics.audio", "sony", Double.box(10.5d), Long.box(3001L), "session-a")
        )
      ),
      ActivitySchema.rawSchema
    )

    val tempDir = Files.createTempDirectory("session-state-store-spec")

    try {
      val sessionized = Sessionizer(Deduplicator(ActivityNormalizer(input, "run-2")))
      val snapshot = SessionStateStore.buildSnapshot(sessionized, "2019-10-01", "run-2")
      val snapshotPath = SessionStateStore.saveSnapshot(snapshot, tempDir.toString, "2019-10-01")
      val reloaded = SessionStateStore.loadSnapshot(spark, tempDir.toString, "2019-10-01")

      assert(snapshotPath.endsWith("snapshot_date_kst=2019-10-01"))
      assert(reloaded.isDefined)
      assert(reloaded.get.count() === 1L)
    } finally {
      Files.walk(tempDir).sorted(java.util.Comparator.reverseOrder()).forEach(path => Files.deleteIfExists(path))
    }
  }
}
