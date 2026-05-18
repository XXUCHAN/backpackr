package query

import org.scalatest.funsuite.AnyFunSuite

class HiveTableManagerSpec extends AnyFunSuite {
  test("renderActivityEventsDdl should replace table name and location") {
    val ddl = HiveTableManager.renderActivityEventsDdl(
      tableName = "activity_events_test",
      location = "/tmp/activity-events-test"
    )

    assert(ddl.contains("CREATE EXTERNAL TABLE IF NOT EXISTS activity_events_test"))
    assert(ddl.contains("LOCATION '/tmp/activity-events-test'"))
    assert(!ddl.contains("LOCATION '/warehouse/activity_events/'"))
  }

  test("buildAddPartitionStatement should create partition registration SQL") {
    val sql = HiveTableManager.buildAddPartitionStatement(
      tableName = "activity_events_test",
      basePath = "/tmp/activity-events-test",
      partitionDate = "2019-10-01"
    )

    assert(sql.contains("ALTER TABLE activity_events_test"))
    assert(sql.contains("PARTITION (event_date_kst='2019-10-01')"))
    assert(sql.contains("LOCATION '/tmp/activity-events-test/event_date_kst=2019-10-01/'"))
  }
}
