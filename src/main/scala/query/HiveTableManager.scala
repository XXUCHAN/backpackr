package query

import org.apache.spark.sql.SparkSession
import support.PathBuilder

import scala.io.Source

object HiveTableManager {
  private val DefaultTableNameToken = "activity_events"
  private val DefaultLocationToken = "/warehouse/activity_events/"

  def createActivityEventsTable(spark: SparkSession, tableName: String, location: String): Unit = {
    spark.sql(renderActivityEventsDdl(tableName, location))
  }

  def addPartitions(
      spark: SparkSession,
      tableName: String,
      basePath: String,
      partitions: Seq[String]
  ): Seq[String] = {
    partitions.distinct.sorted.map { partitionDate =>
      val statement = buildAddPartitionStatement(tableName, basePath, partitionDate)
      spark.sql(statement)
      statement
    }
  }

  def renderActivityEventsDdl(tableName: String, location: String): String = {
    val ddl = loadResource(WauQueries.ActivityEventsDdlResource)

    ddl.replace(s"TABLE IF NOT EXISTS $DefaultTableNameToken", s"TABLE IF NOT EXISTS $tableName")
      .replace(s"LOCATION '$DefaultLocationToken'", s"LOCATION '$location'")
  }

  def buildAddPartitionStatement(tableName: String, basePath: String, partitionDate: String): String =
    s"""ALTER TABLE $tableName
       |ADD IF NOT EXISTS PARTITION (event_date_kst='$partitionDate')
       |LOCATION '${PathBuilder.partitionPath(basePath, partitionDate)}/'""".stripMargin

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
