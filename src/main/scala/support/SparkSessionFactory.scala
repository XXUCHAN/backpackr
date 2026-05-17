package support

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {
  def create(appName: String): SparkSession =
    SparkSession
      .builder()
      .appName(appName)
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .enableHiveSupport()
      .getOrCreate()
}
