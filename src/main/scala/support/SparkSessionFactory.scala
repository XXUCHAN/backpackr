package support

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {
  def create(appName: String): SparkSession = {
    val builder = SparkSession
      .builder()
      .appName(appName)
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .enableHiveSupport()
    val resolvedBuilder =
      if (sys.props.contains("spark.master") || sys.env.contains("SPARK_MASTER") || sys.env.contains("MASTER")) {
        builder
      } else {
        builder.master("local[*]")
      }

    resolvedBuilder.getOrCreate()
  }
}
