package query

import scala.io.Source

object WauQueries {
  val DefaultTableName: String = "activity_events"
  val UserWauResource: String = "sql/wau_users.sql"
  val WeeklyActiveSessionsResource: String = "sql/weekly_active_sessions.sql"
  val ActivityEventsDdlResource: String = "sql/activity_events.ddl.sql"

  def renderUserWauQuery(tableName: String): String =
    loadResource(UserWauResource).replace(DefaultTableName, tableName)

  def renderWeeklyActiveSessionsQuery(tableName: String): String =
    loadResource(WeeklyActiveSessionsResource).replace(DefaultTableName, tableName)

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
