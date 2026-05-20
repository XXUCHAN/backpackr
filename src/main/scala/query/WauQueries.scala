package query

import scala.io.Source

object WauQueries {
  val DefaultTableName: String = "activity_events"
  val UserWauResource: String = "sql/wau_users.sql"
  val WeeklyActiveSessionsResource: String = "sql/weekly_active_sessions.sql"
  val ActivityEventsDdlResource: String = "sql/activity_events.ddl.sql"
  val WauUsersByWeekDdlResource: String = "sql/wau_users_by_week.ddl.sql"
  val WeeklyActiveSessionsByWeekDdlResource: String = "sql/weekly_active_sessions_by_week.ddl.sql"

  def renderUserWauQuery(
      tableName: String,
      affectedWeekStart: Option[String] = None,
      affectedWeekEnd: Option[String] = None
  ): String = {
    val baseQuery = loadResource(UserWauResource).replace(DefaultTableName, tableName)
    val weekFilter = renderWeekFilter("week_start_kst", affectedWeekStart, affectedWeekEnd)

    if (weekFilter.isEmpty) {
      baseQuery
    } else {
      baseQuery.replace(s"FROM $tableName", s"FROM $tableName WHERE $weekFilter")
    }
  }

  def renderWeeklyActiveSessionsQuery(
      tableName: String,
      affectedWeekStart: Option[String] = None,
      affectedWeekEnd: Option[String] = None
  ): String = {
    val baseQuery = loadResource(WeeklyActiveSessionsResource).replace(DefaultTableName, tableName)
    val weekFilter = renderWeekFilter("week_start_kst", affectedWeekStart, affectedWeekEnd)

    if (weekFilter.isEmpty) {
      baseQuery
    } else {
      baseQuery.replace("FROM sessions", s"FROM sessions WHERE $weekFilter")
    }
  }

  private def renderWeekFilter(
      weekExpression: String,
      affectedWeekStart: Option[String],
      affectedWeekEnd: Option[String]
  ): String =
    (affectedWeekStart, affectedWeekEnd) match {
      case (Some(start), Some(end)) => s"$weekExpression BETWEEN DATE '$start' AND DATE '$end'"
      case (Some(start), None)      => s"$weekExpression >= DATE '$start'"
      case (None, Some(end))        => s"$weekExpression <= DATE '$end'"
      case _                        => ""
    }

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
