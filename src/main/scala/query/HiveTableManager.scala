package query

import org.apache.spark.sql.SparkSession
import support.PathBuilder

import java.nio.file.Paths
import scala.io.Source

object HiveTableManager {
  private val DefaultTableNameToken = "activity_events"
  private val DefaultLocationToken = "/warehouse/activity_events/"
  private val DefaultWauUsersTableNameToken = "wau_users_by_week"
  private val DefaultWauUsersLocationToken = "/warehouse/wau_users_by_week/"
  private val DefaultWeeklyActiveSessionsTableNameToken = "weekly_active_sessions_by_week"
  private val DefaultWeeklyActiveSessionsLocationToken = "/warehouse/weekly_active_sessions_by_week/"

  def createActivityEventsTable(spark: SparkSession, tableName: String, location: String): Unit = {
    spark.sql(renderActivityEventsDdl(tableName, resolveAbsolutePath(location)))
  }

  def createWauUsersTable(spark: SparkSession, tableName: String, location: String): Unit = {
    spark.sql(
      renderDdl(
        resourcePath = WauQueries.WauUsersByWeekDdlResource,
        defaultTableName = DefaultWauUsersTableNameToken,
        defaultLocation = DefaultWauUsersLocationToken,
        tableName = tableName,
        location = resolveAbsolutePath(location)
      )
    )
  }

  def createWeeklyActiveSessionsTable(spark: SparkSession, tableName: String, location: String): Unit = {
    spark.sql(
      renderDdl(
        resourcePath = WauQueries.WeeklyActiveSessionsByWeekDdlResource,
        defaultTableName = DefaultWeeklyActiveSessionsTableNameToken,
        defaultLocation = DefaultWeeklyActiveSessionsLocationToken,
        tableName = tableName,
        location = resolveAbsolutePath(location)
      )
    )
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
    renderDdl(
      resourcePath = WauQueries.ActivityEventsDdlResource,
      defaultTableName = DefaultTableNameToken,
      defaultLocation = DefaultLocationToken,
      tableName = tableName,
      location = location
    )
  }

  def renderWauUsersDdl(tableName: String, location: String): String =
    renderDdl(
      resourcePath = WauQueries.WauUsersByWeekDdlResource,
      defaultTableName = DefaultWauUsersTableNameToken,
      defaultLocation = DefaultWauUsersLocationToken,
      tableName = tableName,
      location = location
    )

  def renderWeeklyActiveSessionsDdl(tableName: String, location: String): String =
    renderDdl(
      resourcePath = WauQueries.WeeklyActiveSessionsByWeekDdlResource,
      defaultTableName = DefaultWeeklyActiveSessionsTableNameToken,
      defaultLocation = DefaultWeeklyActiveSessionsLocationToken,
      tableName = tableName,
      location = location
    )

  def buildAddPartitionStatement(tableName: String, basePath: String, partitionDate: String): String =
    s"""ALTER TABLE $tableName
       |ADD IF NOT EXISTS PARTITION (event_date_kst='$partitionDate')
       |LOCATION '${resolvePartitionPath(basePath, partitionDate)}/'""".stripMargin

  def buildSetPartitionLocationStatement(tableName: String, basePath: String, partitionDate: String): String =
    s"""ALTER TABLE $tableName
       |PARTITION (event_date_kst='$partitionDate')
       |SET LOCATION '${resolvePartitionPath(basePath, partitionDate)}/'""".stripMargin

  def addWeekPartitions(
      spark: SparkSession,
      tableName: String,
      basePath: String,
      partitions: Seq[String]
  ): Seq[String] = {
    val resolvedBasePath = resolveAbsolutePath(basePath)

    val registeredPartitions = partitions.distinct.sorted.map { partitionDate =>
      spark.sql(buildAddWeekPartitionStatement(tableName, resolvedBasePath, partitionDate))
      spark.sql(buildSetWeekPartitionLocationStatement(tableName, resolvedBasePath, partitionDate))
      partitionDate
    }

    spark.catalog.refreshTable(tableName)
    registeredPartitions
  }

  def buildAddWeekPartitionStatement(tableName: String, basePath: String, partitionDate: String): String =
    s"""ALTER TABLE $tableName
       |ADD IF NOT EXISTS PARTITION (week_start_kst='$partitionDate')
       |LOCATION '${resolveWeekPartitionPath(basePath, partitionDate)}/'""".stripMargin

  def buildSetWeekPartitionLocationStatement(tableName: String, basePath: String, partitionDate: String): String =
    s"""ALTER TABLE $tableName
       |PARTITION (week_start_kst='$partitionDate')
       |SET LOCATION '${resolveWeekPartitionPath(basePath, partitionDate)}/'""".stripMargin

  private def resolvePartitionPath(basePath: String, partitionDate: String): String =
    resolveAbsolutePath(PathBuilder.partitionPath(basePath, partitionDate))

  private def resolveWeekPartitionPath(basePath: String, partitionDate: String): String =
    resolveAbsolutePath(s"$basePath/week_start_kst=$partitionDate")

  private def renderDdl(
      resourcePath: String,
      defaultTableName: String,
      defaultLocation: String,
      tableName: String,
      location: String
  ): String = {
    val ddl = loadResource(resourcePath)

    ddl.replace(s"TABLE IF NOT EXISTS $defaultTableName", s"TABLE IF NOT EXISTS $tableName")
      .replace(s"LOCATION '$defaultLocation'", s"LOCATION '$location'")
  }

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
