package support

import config.AppConfig

import java.nio.file.{Files, Paths}
import java.time.{LocalDate, ZoneId}
import java.time.format.DateTimeFormatter

object PreflightValidator {
  private val KstZoneId = ZoneId.of("Asia/Seoul")
  private val DateFormatter = DateTimeFormatter.ISO_LOCAL_DATE

  def validate(config: AppConfig): Unit = {
    val startDate = parseDate(config.startDate, "start-date")
    val endDate = parseDate(config.endDate, "end-date")
    val todayKst = LocalDate.now(KstZoneId)

    require(!startDate.isAfter(endDate), s"start-date must be on or before end-date: ${config.startDate} > ${config.endDate}")
    require(!startDate.isAfter(todayKst), s"start-date must not be in the future: ${config.startDate}")
    require(!endDate.isAfter(todayKst), s"end-date must not be in the future: ${config.endDate}")

    val inputPath = Paths.get(config.inputPath)
    require(Files.exists(inputPath), s"input-path does not exist: ${config.inputPath}")

    val stagingRunPath = Paths.get(PathBuilder.stagingRunPath(config.stagingBasePath, config.runId))
    require(!Files.exists(stagingRunPath), s"staging path for run_id already exists: $stagingRunPath")

    val dlqRunPath = Paths.get(PathBuilder.stagingRunPath(config.dlqBasePath, config.runId))
    require(!Files.exists(dlqRunPath), s"dlq path for run_id already exists: $dlqRunPath")

    val runLogPath = Paths.get(PathBuilder.batchRunLogPath(config.runLogBasePath, config.runId))
    require(!Files.exists(runLogPath), s"batch run log path for run_id already exists: $runLogPath")

    if (config.registerHivePartitions) {
      require(
        Option(config.outputBasePath).exists(_.trim.nonEmpty),
        "output-base-path must be provided when register-hive-partitions is enabled"
      )
    }
  }

  private def parseDate(value: String, fieldName: String): LocalDate =
    try {
      LocalDate.parse(value, DateFormatter)
    } catch {
      case _: Exception =>
        throw new IllegalArgumentException(s"$fieldName must be in yyyy-MM-dd format: $value")
    }
}
