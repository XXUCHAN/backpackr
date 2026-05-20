package support

import java.time.LocalDate
import java.time.temporal.TemporalAdjusters
import java.time.DayOfWeek.MONDAY
import scala.annotation.tailrec

object WeekRangeCalculator {
  def toWeekStart(date: String): String =
    LocalDate.parse(date).`with`(TemporalAdjusters.previousOrSame(MONDAY)).toString

  def affectedWeekRange(startDate: String, endDate: String): (String, String) =
    (toWeekStart(startDate), toWeekStart(endDate))

  def affectedWeeks(startDate: String, endDate: String): Seq[String] = {
    val startWeek = LocalDate.parse(toWeekStart(startDate))
    val endWeek = LocalDate.parse(toWeekStart(endDate))

    @tailrec
    def loop(cursor: LocalDate, acc: Vector[String]): Vector[String] =
      if (cursor.isAfter(endWeek)) acc
      else loop(cursor.plusWeeks(1), acc :+ cursor.toString)

    loop(startWeek, Vector.empty)
  }
}
