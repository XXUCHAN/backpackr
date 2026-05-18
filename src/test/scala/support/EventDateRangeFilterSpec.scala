package support

import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite
import schema.ActivitySchema
import transform.ActivityNormalizer

class EventDateRangeFilterSpec extends SparkFunSuite {
  test("event date range filter should keep only rows within KST date range") {
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row("2019-10-01 00:10:00 UTC", "view", Long.box(1L), Long.box(10L), "a", "brand-a", Double.box(9.99d), Long.box(101L), "session-1"),
          Row("2019-10-06 13:00:00 UTC", "view", Long.box(2L), Long.box(10L), "b", "brand-b", Double.box(19.99d), Long.box(102L), "session-2"),
          Row("2019-10-06 15:30:00 UTC", "view", Long.box(3L), Long.box(10L), "c", "brand-c", Double.box(29.99d), Long.box(103L), "session-3")
        )
      ),
      ActivitySchema.rawSchema
    )

    val normalized = ActivityNormalizer(input, "run-1")
    val filtered = EventDateRangeFilter.filter(normalized, "2019-10-01", "2019-10-06")

    val productIds = filtered.select("product_id").collect().map(_.getLong(0)).toSet

    assert(productIds === Set(1L, 2L))
  }
}
