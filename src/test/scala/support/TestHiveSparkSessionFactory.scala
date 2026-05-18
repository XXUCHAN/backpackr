package support

import org.apache.spark.sql.SparkSession

import java.nio.file.Path

object TestHiveSparkSessionFactory {
  private val RequiredJavaSpecVersion = "17"

  def create(appName: String, warehouseDir: Path, metastoreDir: Path): SparkSession = {
    val actualJavaSpecVersion = sys.props.getOrElse("java.specification.version", "unknown")
    require(
      actualJavaSpecVersion == RequiredJavaSpecVersion,
      s"Spark Hive tests must run on Java $RequiredJavaSpecVersion, but current java.specification.version is $actualJavaSpecVersion. " +
        "Run tests with JAVA_HOME set to /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home."
    )

    SparkSession
      .builder()
      .master("local[1]")
      .appName(appName)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.catalogImplementation", "hive")
      .config("spark.sql.warehouse.dir", warehouseDir.toAbsolutePath.toString)
      .config("hive.metastore.warehouse.dir", warehouseDir.toAbsolutePath.toString)
      .config(
        "javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=${metastoreDir.toAbsolutePath.toString};create=true"
      )
      .enableHiveSupport()
      .getOrCreate()
  }
}
