package support

import org.apache.spark.sql.SparkSession

object TestSparkSessionFactory {
  private val RequiredJavaSpecVersion = "17"

  def create(appName: String = "activity-etl-wau-test"): SparkSession = {
    val actualJavaSpecVersion = sys.props.getOrElse("java.specification.version", "unknown")
    require(
      actualJavaSpecVersion == RequiredJavaSpecVersion,
      s"Spark tests must run on Java $RequiredJavaSpecVersion, but current java.specification.version is $actualJavaSpecVersion. " +
        "Run tests with JAVA_HOME set to /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home."
    )

    SparkSession
      .builder()
      .master("local[1]")
      .appName(appName)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  }
}
