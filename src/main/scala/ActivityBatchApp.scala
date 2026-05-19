import config.AppConfigParser
import logging.BatchRunLogger
import model.{BatchExecutionSummary, BatchRunStatus}
import org.apache.spark.sql.functions.{col, coalesce, lit, not}
import query.{HiveTableManager, WauQueryExecutor}
import reader.CsvActivityReader
import sessionization.{SessionStateStore, Sessionizer}
import support.{EventDateRangeFilter, PathBuilder, PreflightValidator, QualityGate, SparkSessionFactory}
import transform.{ActivityNormalizer, Deduplicator, Validator}
import writer.{ActivityWriter, DlqWriter}

import java.time.Instant
import java.time.LocalDate
import java.sql.Date

object ActivityBatchApp {
  def main(args: Array[String]): Unit = {
    val config = AppConfigParser.parse(args)
    val startedAt = Instant.now
    val targetDate = config.startDate
    val previousSnapshotDate = LocalDate.parse(targetDate).minusDays(1).toString
    val validOutputPath = s"${PathBuilder.stagingRunPath(config.stagingBasePath, config.runId)}/valid"
    val dlqOutputPath = s"${PathBuilder.stagingRunPath(config.dlqBasePath, config.runId)}/invalid"
    val finalOutputPath = Option(config.outputBasePath).map(_.trim).filter(_.nonEmpty)
    val shouldRegisterHivePartitions = config.registerHivePartitions || config.executeWau
    val wauRunPath = PathBuilder.wauRunPath(config.wauOutputBasePath, config.runId)
    val wauUsersOutputPath = s"$wauRunPath/wau-users"
    val weeklyActiveSessionsOutputPath = s"$wauRunPath/weekly-active-sessions"

    try {
      PreflightValidator.validate(config)
      BatchRunLogger.logStatus(
        runLogBasePath = config.runLogBasePath,
        runId = config.runId,
        targetDate = targetDate,
        status = BatchRunStatus.Running,
        startedAt = startedAt,
        stagingPath = Some(validOutputPath),
        dlqPath = Some(dlqOutputPath),
        finalOutputPath = finalOutputPath,
        processingStartDate = Some(config.startDate),
        processingEndDate = Some(config.endDate),
        snapshotSeedDate = Some(previousSnapshotDate),
        snapshotTargetDate = Some(config.endDate)
      )

      val spark = SparkSessionFactory.create(config.appName)

      try {
        spark.sparkContext.setLogLevel("WARN")

        val raw = CsvActivityReader.read(spark, config.inputPath)
        val normalized = ActivityNormalizer(raw, config.runId)
        val targetDateColumn = coalesce(col("event_date_kst").cast("string"), lit(config.startDate))
        val validationResult = Validator(normalized, targetDateColumn)
        val rangeFilteredValid = EventDateRangeFilter.filter(validationResult.valid, config.startDate, config.endDate)
        val deduplicationResult = Deduplicator.analyze(rangeFilteredValid)
        val deduplicatedValid = deduplicationResult.deduplicated
        val previousSnapshot = SessionStateStore.loadSnapshot(spark, config.sessionStateBasePath, previousSnapshotDate)
        val sessionizedValid = Sessionizer(deduplicatedValid, previousSnapshot)
        val duplicates = deduplicationResult.duplicates

        ActivityWriter.writeToStaging(sessionizedValid, validOutputPath)
        DlqWriter.write(validationResult.invalid, dlqOutputPath)

        val inputCount = raw.count()
        val validCount = rangeFilteredValid.count()
        val sessionizedCount = sessionizedValid.count()
        val invalidCount = validationResult.invalid.count()
        val duplicateRowsCount = duplicates.count()
        val droppedDuplicateRowsCount = duplicates.filter(not(col("dedup_retained"))).count()
        val duplicateGroupCount = if (duplicateRowsCount > 0L) duplicates.select("dedup_key").distinct().count() else 0L
        val uniqueSessionCount = sessionizedValid.select("session_id").distinct().count()
        val invalidReasonSummary =
          if (invalidCount > 0L) {
            validationResult.invalid
              .groupBy("reject_reason")
              .count()
              .collect()
              .map(row => row.getString(0) -> row.getLong(1))
              .toMap
          } else {
            Map.empty[String, Long]
          }
        val outputPartitions = sessionizedValid
          .select("event_date_kst")
          .distinct()
          .orderBy(col("event_date_kst"))
          .collect()
          .map(_.getDate(0).toString)
          .toSeq
        val dlqRatio = if (inputCount == 0L) 0.0d else invalidCount.toDouble / inputCount.toDouble

        val summary = BatchExecutionSummary(
          inputRowCount = inputCount,
          validRowCount = validCount,
          invalidRowCount = invalidCount,
          outputRowCount = sessionizedCount,
          duplicateGroupCount = duplicateGroupCount,
          duplicateRowsCount = duplicateRowsCount,
          droppedDuplicateRowsCount = droppedDuplicateRowsCount,
          dlqRatio = dlqRatio,
          invalidReasonSummary = invalidReasonSummary,
          outputPartitions = outputPartitions
        )

        val qualityGateResult = QualityGate.evaluate(summary)

        BatchRunLogger.logStatus(
          runLogBasePath = config.runLogBasePath,
          runId = config.runId,
          targetDate = targetDate,
          status = BatchRunStatus.Validated,
          startedAt = startedAt,
          stagingPath = Some(validOutputPath),
          dlqPath = Some(dlqOutputPath),
          finalOutputPath = finalOutputPath,
          summary = Some(summary),
          message =
            if (qualityGateResult.warnings.nonEmpty) {
              Some(s"${qualityGateResult.warnings.mkString("; ")}; unique_session_count=$uniqueSessionCount")
            } else {
              Some(s"unique_session_count=$uniqueSessionCount")
            },
          processingStartDate = Some(config.startDate),
          processingEndDate = Some(config.endDate),
          snapshotSeedDate = Some(previousSnapshotDate),
          snapshotTargetDate = Some(config.endDate),
          qualityGateWarnings = qualityGateResult.warnings,
          uniqueSessionCount = Some(uniqueSessionCount)
        )

        finalOutputPath.foreach { outputPath =>
          ActivityWriter.promoteToFinal(validOutputPath, outputPath, outputPartitions)
          ActivityWriter.cleanupPath(validOutputPath)
          BatchRunLogger.logStatus(
            runLogBasePath = config.runLogBasePath,
            runId = config.runId,
            targetDate = targetDate,
            status = BatchRunStatus.Promoted,
            startedAt = startedAt,
            stagingPath = Some(validOutputPath),
            dlqPath = Some(dlqOutputPath),
            finalOutputPath = Some(outputPath),
            summary = Some(summary),
            message =
              Some(
                s"unique_session_count=$uniqueSessionCount; promoted_partitions=${outputPartitions.mkString(",")}"
              ),
            processingStartDate = Some(config.startDate),
            processingEndDate = Some(config.endDate),
            snapshotSeedDate = Some(previousSnapshotDate),
            snapshotTargetDate = Some(config.endDate),
            qualityGateWarnings = qualityGateResult.warnings,
            uniqueSessionCount = Some(uniqueSessionCount)
          )
        }

        val snapshotTargetDate = config.endDate

        val sessionSnapshot = SessionStateStore.buildSnapshot(sessionizedValid, snapshotTargetDate, config.runId)
        val sessionSnapshotPath =
          SessionStateStore.saveSnapshot(sessionSnapshot, config.sessionStateBasePath, snapshotTargetDate)

        val registeredHivePartitions = finalOutputPath match {
          case Some(outputPath) if shouldRegisterHivePartitions =>
            HiveTableManager.createActivityEventsTable(spark, config.hiveTableName, outputPath)
            HiveTableManager.addPartitions(spark, config.hiveTableName, outputPath, outputPartitions)
          case _ =>
            Seq.empty[String]
        }

        val wauSummary =
          if (config.executeWau) {
            val userWau = WauQueryExecutor.runUserWau(spark, config.hiveTableName)
            val weeklyActiveSessions = WauQueryExecutor.runWeeklyActiveSessions(spark, config.hiveTableName)

            WauQueryExecutor.writeResult(userWau, wauUsersOutputPath)
            WauQueryExecutor.writeResult(weeklyActiveSessions, weeklyActiveSessionsOutputPath)

            val userWauRows = userWau.collect().toSeq
            val weeklyActiveSessionRows = weeklyActiveSessions.collect().toSeq
            val userWauWeekCount = userWauRows.size.toLong
            val weeklyActiveSessionWeekCount = weeklyActiveSessionRows.size.toLong
            val userWauStartWeek = userWauRows.headOption.map(row => row.getAs[Date]("week_start_kst").toString)
            val userWauEndWeek = userWauRows.lastOption.map(row => row.getAs[Date]("week_start_kst").toString)
            val weeklyActiveSessionsStartWeek =
              weeklyActiveSessionRows.headOption.map(row => row.getAs[Date]("week_start_kst").toString)
            val weeklyActiveSessionsEndWeek =
              weeklyActiveSessionRows.lastOption.map(row => row.getAs[Date]("week_start_kst").toString)

            println("wau_users:")
            userWau.show(100, truncate = false)
            println("weekly_active_sessions:")
            weeklyActiveSessions.show(100, truncate = false)
            println(
              s"wau_summary=week_count=$userWauWeekCount start_week=${userWauStartWeek.getOrElse("n/a")} end_week=${userWauEndWeek.getOrElse("n/a")}"
            )
            println(
              s"weekly_active_sessions_summary=week_count=$weeklyActiveSessionWeekCount start_week=${weeklyActiveSessionsStartWeek.getOrElse("n/a")} end_week=${weeklyActiveSessionsEndWeek.getOrElse("n/a")}"
            )

            Some(
              WauExecutionSummary(
                wauUsersOutputPath = wauUsersOutputPath,
                weeklyActiveSessionsOutputPath = weeklyActiveSessionsOutputPath,
                wauUsersWeekCount = userWauWeekCount,
                wauUsersStartWeek = userWauStartWeek,
                wauUsersEndWeek = userWauEndWeek,
                weeklyActiveSessionsWeekCount = weeklyActiveSessionWeekCount,
                weeklyActiveSessionsStartWeek = weeklyActiveSessionsStartWeek,
                weeklyActiveSessionsEndWeek = weeklyActiveSessionsEndWeek
              )
            )
          } else {
            None
          }

        BatchRunLogger.logStatus(
          runLogBasePath = config.runLogBasePath,
          runId = config.runId,
          targetDate = targetDate,
          status = BatchRunStatus.Success,
          startedAt = startedAt,
          stagingPath = Some(validOutputPath),
          dlqPath = Some(dlqOutputPath),
          finalOutputPath = finalOutputPath,
          summary = Some(summary),
          message =
            if (qualityGateResult.warnings.nonEmpty) {
              Some(
                s"${qualityGateResult.warnings.mkString("; ")}; unique_session_count=$uniqueSessionCount"
              )
            } else {
              Some(s"unique_session_count=$uniqueSessionCount")
            },
          processingStartDate = Some(config.startDate),
          processingEndDate = Some(config.endDate),
          snapshotSeedDate = Some(previousSnapshotDate),
          snapshotTargetDate = Some(config.endDate),
          qualityGateWarnings = qualityGateResult.warnings,
          uniqueSessionCount = Some(uniqueSessionCount),
          registeredHivePartitionsCount = Some(registeredHivePartitions.size),
          sessionSnapshotPath = Some(sessionSnapshotPath),
          wauUsersOutputPath = wauSummary.map(_.wauUsersOutputPath),
          weeklyActiveSessionsOutputPath = wauSummary.map(_.weeklyActiveSessionsOutputPath),
          wauUsersWeekCount = wauSummary.map(_.wauUsersWeekCount),
          wauUsersStartWeek = wauSummary.flatMap(_.wauUsersStartWeek),
          wauUsersEndWeek = wauSummary.flatMap(_.wauUsersEndWeek),
          weeklyActiveSessionsWeekCount = wauSummary.map(_.weeklyActiveSessionsWeekCount),
          weeklyActiveSessionsStartWeek = wauSummary.flatMap(_.weeklyActiveSessionsStartWeek),
          weeklyActiveSessionsEndWeek = wauSummary.flatMap(_.weeklyActiveSessionsEndWeek)
        )

        println(s"mode=${config.mode.entryName}")
        println(s"run_id=${config.runId}")
        println(s"input_path=${config.inputPath}")
        println(s"valid_output_path=$validOutputPath")
        println(s"dlq_output_path=$dlqOutputPath")
        println(s"session_snapshot_path=$sessionSnapshotPath")
        println(s"registered_hive_partitions_count=${registeredHivePartitions.size}")
        finalOutputPath.foreach(path => println(s"final_output_path=$path"))
        if (config.executeWau) {
          println(s"wau_users_output_path=$wauUsersOutputPath")
          println(s"weekly_active_sessions_output_path=$weeklyActiveSessionsOutputPath")
        }
        println(s"input_row_count=$inputCount")
        println(s"validated_row_count=$validCount")
        println(s"sessionized_row_count=$sessionizedCount")
        println(s"unique_session_count=$uniqueSessionCount")
        println(s"duplicate_group_count=$duplicateGroupCount")
        println(s"duplicate_rows_count=$duplicateRowsCount")
        println(s"dropped_duplicate_row_count=$droppedDuplicateRowsCount")
        println(s"invalid_row_count=$invalidCount")
        println(f"dlq_ratio=$dlqRatio%.4f")
        println(s"output_partitions=${outputPartitions.mkString(",")}")

        if (invalidCount > 0) {
          println("invalid_reason_summary:")
          validationResult.invalid
            .groupBy("reject_reason")
            .count()
            .orderBy(col("count").desc, col("reject_reason"))
            .show(20, truncate = false)
        }

        if (qualityGateResult.warnings.nonEmpty) {
          println(s"quality_gate_warnings=${qualityGateResult.warnings.mkString("; ")}")
        }
      } finally {
        spark.stop()
      }
    } catch {
      case error: Exception =>
        BatchRunLogger.logStatus(
          runLogBasePath = config.runLogBasePath,
          runId = config.runId,
          targetDate = targetDate,
          status = BatchRunStatus.Failed,
          startedAt = startedAt,
          stagingPath = Some(validOutputPath),
          dlqPath = Some(dlqOutputPath),
          finalOutputPath = finalOutputPath,
          message = Some(error.getMessage),
          processingStartDate = Some(config.startDate),
          processingEndDate = Some(config.endDate),
          snapshotSeedDate = Some(previousSnapshotDate),
          snapshotTargetDate = Some(config.endDate)
        )
        throw error
    }
  }

  private final case class WauExecutionSummary(
      wauUsersOutputPath: String,
      weeklyActiveSessionsOutputPath: String,
      wauUsersWeekCount: Long,
      wauUsersStartWeek: Option[String],
      wauUsersEndWeek: Option[String],
      weeklyActiveSessionsWeekCount: Long,
      weeklyActiveSessionsStartWeek: Option[String],
      weeklyActiveSessionsEndWeek: Option[String]
  )
}
