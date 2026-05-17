package support

import org.apache.spark.sql.SparkSession

object TestSparkSessionFactory {
  def create(appName: String = "activity-etl-wau-test"): SparkSession =
    SparkSession
      .builder()
      .master("local[1]")
      .appName(appName)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
}
