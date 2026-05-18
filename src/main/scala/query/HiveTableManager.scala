package query

import org.apache.spark.sql.SparkSession
import support.PathBuilder

import java.nio.file.Paths
import scala.io.Source

object HiveTableManager {
  private val DefaultTableNameToken = "activity_events"
  private val DefaultLocationToken = "/warehouse/activity_events/"

  def createActivityEventsTable(spark: SparkSession, tableName: String, location: String): Unit = {
    spark.sql(renderActivityEventsDdl(tableName, resolveAbsolutePath(location)))
  }

  def addPartitions(
      spark: SparkSession,
      tableName: String,
      basePath: String,
      partitions: Seq[String]
  ): Seq[String] = {
    val resolvedBasePath = resolveAbsolutePath(basePath)

    val registeredPartitions = partitions.distinct.sorted.map { partitionDate =>
      spark.sql(buildAddPartitionStatement(tableName, resolvedBasePath, partitionDate))
      spark.sql(buildSetPartitionLocationStatement(tableName, resolvedBasePath, partitionDate))
      partitionDate
    }

    spark.catalog.refreshTable(tableName)
    registeredPartitions
  }

  def renderActivityEventsDdl(tableName: String, location: String): String = {
    val ddl = loadResource(WauQueries.ActivityEventsDdlResource)

    ddl.replace(s"TABLE IF NOT EXISTS $DefaultTableNameToken", s"TABLE IF NOT EXISTS $tableName")
      .replace(s"LOCATION '$DefaultLocationToken'", s"LOCATION '$location'")
  }

  def buildAddPartitionStatement(tableName: String, basePath: String, partitionDate: String): String =
    s"""ALTER TABLE $tableName
       |ADD IF NOT EXISTS PARTITION (event_date_kst='$partitionDate')
       |LOCATION '${resolvePartitionPath(basePath, partitionDate)}/'""".stripMargin

  def buildSetPartitionLocationStatement(tableName: String, basePath: String, partitionDate: String): String =
    s"""ALTER TABLE $tableName
       |PARTITION (event_date_kst='$partitionDate')
       |SET LOCATION '${resolvePartitionPath(basePath, partitionDate)}/'""".stripMargin

  private def resolvePartitionPath(basePath: String, partitionDate: String): String =
    resolveAbsolutePath(PathBuilder.partitionPath(basePath, partitionDate))

  private def resolveAbsolutePath(path: String): String =
    Paths.get(path).toAbsolutePath.normalize.toString

  private def loadResource(resourcePath: String): String = {
    val stream = Option(getClass.getClassLoader.getResourceAsStream(resourcePath))
      .getOrElse(throw new IllegalArgumentException(s"Missing resource: $resourcePath"))

    try {
      Source.fromInputStream(stream, "UTF-8").mkString
    } finally {
      stream.close()
    }
  }
}
