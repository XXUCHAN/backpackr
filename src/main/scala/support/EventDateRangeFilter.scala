package support

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

import java.sql.Date

object EventDateRangeFilter {
  def filter(df: DataFrame, startDate: String, endDate: String): DataFrame =
    df.filter(
      col("event_date_kst").between(
        lit(Date.valueOf(startDate)),
        lit(Date.valueOf(endDate))
      )
    )
}
