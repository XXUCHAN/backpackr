import config.AppConfigParser
import logging.BatchRunLogger
import model.{BatchExecutionSummary, BatchRunStatus}
import org.apache.spark.sql.functions.{coalesce, col, count, lit, sum}
import org.apache.spark.storage.StorageLevel
import query.{HiveTableManager, WauQueryExecutor}
import reader.CsvActivityReader
import sessionization.{SessionStateStore, Sessionizer}
import support.{EventDateRangeFilter, PathBuilder, PreflightValidator, QualityGate, SparkSessionFactory, WeekRangeCalculator}
import transform.{ActivityNormalizer, Deduplicator, Validator}
import writer.{ActivityWriter, DlqWriter}

import java.sql.Date
import java.time.{Instant, LocalDate}

object ActivityBatchApp {
  private val WauUsersTableName = "wau_users_by_week"
  private val WeeklyActiveSessionsTableName = "weekly_active_sessions_by_week"

  def main(args: Array[String]): Unit = {
    val config = AppConfigParser.parse(args)
    val startedAt = Instant.now
    val targetDate = config.startDate
    val previousSnapshotDate = LocalDate.parse(targetDate).minusDays(1).toString
    val validOutputPath = s"${PathBuilder.stagingRunPath(config.stagingBasePath, config.runId)}/valid"
    val dlqOutputPath = s"${PathBuilder.stagingRunPath(config.dlqBasePath, config.runId)}/invalid"
    val finalOutputPath = config.outputBasePath.trim
    val (affectedWeekStart, affectedWeekEnd) =
      WeekRangeCalculator.affectedWeekRange(config.startDate, config.endDate)
    val affectedWeeks = WeekRangeCalculator.affectedWeeks(config.startDate, config.endDate)
    val wauUsersOutputPath = s"${config.wauOutputBasePath}/wau-users-by-week"
    val weeklyActiveSessionsOutputPath = s"${config.wauOutputBasePath}/weekly-active-sessions-by-week"

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
        finalOutputPath = Some(finalOutputPath),
        processingStartDate = Some(config.startDate),
        processingEndDate = Some(config.endDate),
        snapshotSeedDate = Some(previousSnapshotDate),
        snapshotTargetDate = Some(config.endDate)
      )
      println(s"batch_status=${BatchRunStatus.Running.entryName}")

      val spark = SparkSessionFactory.create(config.appName)

      try {
        spark.sparkContext.setLogLevel("WARN")

        val raw = CsvActivityReader.read(spark, config.inputPath)

        BatchRunLogger.logStatus(
          runLogBasePath = config.runLogBasePath,
          runId = config.runId,
          targetDate = targetDate,
          status = BatchRunStatus.Normalized,
          startedAt = startedAt,
          stagingPath = Some(validOutputPath),
          dlqPath = Some(dlqOutputPath),
          finalOutputPath = Some(finalOutputPath),
          processingStartDate = Some(config.startDate),
          processingEndDate = Some(config.endDate),
          snapshotSeedDate = Some(previousSnapshotDate),
          snapshotTargetDate = Some(config.endDate)
        )
        println(s"batch_status=${BatchRunStatus.Normalized.entryName}")

        val normalized = ActivityNormalizer(raw, config.runId).persist(StorageLevel.DISK_ONLY)
        val inputCount = normalized.count()

        BatchRunLogger.logStatus(
          runLogBasePath = config.runLogBasePath,
          runId = config.runId,
          targetDate = targetDate,
          status = BatchRunStatus.Validated,
          startedAt = startedAt,
          stagingPath = Some(validOutputPath),
          dlqPath = Some(dlqOutputPath),
          finalOutputPath = Some(finalOutputPath),
          processingStartDate = Some(config.startDate),
          processingEndDate = Some(config.endDate),
          snapshotSeedDate = Some(previousSnapshotDate),
          snapshotTargetDate = Some(config.endDate),
          inputRowCount = Some(inputCount)
        )
        println(s"batch_status=${BatchRunStatus.Validated.entryName}")

        val targetDateColumn = coalesce(col("event_date_kst").cast("string"), lit(config.startDate))
        val validationResult = Validator(normalized, targetDateColumn)
        val invalid = validationResult.invalid.persist(StorageLevel.MEMORY_AND_DISK)
        val rangeFilteredValid =
          EventDateRangeFilter.filter(validationResult.valid, config.startDate, config.endDate).persist(StorageLevel.DISK_ONLY)
        val validCount = rangeFilteredValid.count()
        val invalidCount = invalid.count()
        val invalidReasonSummary =
          if (invalidCount > 0L) {
            invalid
              .groupBy("reject_reason")
              .count()
              .collect()
              .map(row => row.getString(0) -> row.getLong(1))
              .toMap
          } else {
            Map.empty[String, Long]
          }
        val dlqRatio = if (inputCount == 0L) 0.0d else invalidCount.toDouble / inputCount.toDouble
        normalized.unpersist(blocking = false)

        BatchRunLogger.logStatus(
          runLogBasePath = config.runLogBasePath,
          runId = config.runId,
          targetDate = targetDate,
          status = BatchRunStatus.Deduplicated,
          startedAt = startedAt,
          stagingPath = Some(validOutputPath),
          dlqPath = Some(dlqOutputPath),
          finalOutputPath = Some(finalOutputPath),
          processingStartDate = Some(config.startDate),
          processingEndDate = Some(config.endDate),
          snapshotSeedDate = Some(previousSnapshotDate),
          snapshotTargetDate = Some(config.endDate),
          validRowCount = Some(validCount),
          invalidRowCount = Some(invalidCount),
          dlqRatio = Some(dlqRatio),
          invalidReasonSummary = invalidReasonSummary
        )
        println(s"batch_status=${BatchRunStatus.Deduplicated.entryName}")

        val deduplicationResult = Deduplicator.analyze(rangeFilteredValid)
        val deduplicatedValid = deduplicationResult.deduplicated.persist(StorageLevel.DISK_ONLY)
        val duplicateGroups = deduplicationResult.duplicateGroups.persist(StorageLevel.DISK_ONLY)
        val deduplicatedCount = deduplicatedValid.count()
        val duplicateMetrics = duplicateGroups
          .agg(
            count(lit(1)).as("duplicate_group_count"),
            coalesce(sum(col("duplicate_group_size")), lit(0L)).cast("long").as("duplicate_rows_count"),
            coalesce(sum(col("dropped_duplicate_row_count")), lit(0L)).cast("long").as("dropped_duplicate_rows_count")
          )
          .first()
        val duplicateGroupCount = duplicateMetrics.getAs[Long]("duplicate_group_count")
        val duplicateRowsCount = duplicateMetrics.getAs[Long]("duplicate_rows_count")
        val droppedDuplicateRowsCount = duplicateMetrics.getAs[Long]("dropped_duplicate_rows_count")
        rangeFilteredValid.unpersist(blocking = false)

        BatchRunLogger.logStatus(
          runLogBasePath = config.runLogBasePath,
          runId = config.runId,
          targetDate = targetDate,
          status = BatchRunStatus.Sessionized,
          startedAt = startedAt,
          stagingPath = Some(validOutputPath),
          dlqPath = Some(dlqOutputPath),
          finalOutputPath = Some(finalOutputPath),
          processingStartDate = Some(config.startDate),
          processingEndDate = Some(config.endDate),
          snapshotSeedDate = Some(previousSnapshotDate),
          snapshotTargetDate = Some(config.endDate),
          outputRowCount = Some(deduplicatedCount),
          duplicateGroupCount = Some(duplicateGroupCount),
          duplicateRowsCount = Some(duplicateRowsCount),
          droppedDuplicateRowsCount = Some(droppedDuplicateRowsCount)
        )
        println(s"batch_status=${BatchRunStatus.Sessionized.entryName}")

        val previousSnapshot = SessionStateStore.loadSnapshot(spark, config.sessionStateBasePath, previousSnapshotDate)
        val sessionizedValid = Sessionizer(deduplicatedValid, previousSnapshot).persist(StorageLevel.DISK_ONLY)
        val sessionizedCount = sessionizedValid.count()
        val uniqueSessionCount = sessionizedValid.select("session_id").distinct().count()
        val outputPartitions = sessionizedValid
          .select("event_date_kst")
          .distinct()
          .orderBy(col("event_date_kst"))
          .collect()
          .map(_.getDate(0).toString)
          .toSeq
        val outputPartitionCount = outputPartitions.size
        val outputPartitionStart = outputPartitions.headOption
        val outputPartitionEnd = outputPartitions.lastOption
        deduplicatedValid.unpersist(blocking = false)

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
          status = BatchRunStatus.Promoted,
          startedAt = startedAt,
          stagingPath = Some(validOutputPath),
          dlqPath = Some(dlqOutputPath),
          finalOutputPath = Some(finalOutputPath),
          processingStartDate = Some(config.startDate),
          processingEndDate = Some(config.endDate),
          snapshotSeedDate = Some(previousSnapshotDate),
          snapshotTargetDate = Some(config.endDate),
          qualityGateWarnings = qualityGateResult.warnings,
          outputRowCount = Some(sessionizedCount),
          uniqueSessionCount = Some(uniqueSessionCount),
          outputPartitionCount = Some(outputPartitionCount),
          outputPartitionStart = outputPartitionStart,
          outputPartitionEnd = outputPartitionEnd
        )
        println(s"batch_status=${BatchRunStatus.Promoted.entryName}")

        ActivityWriter.writeToStaging(sessionizedValid, validOutputPath)
        DlqWriter.write(invalid, dlqOutputPath)

        ActivityWriter.promoteToFinal(validOutputPath, finalOutputPath, outputPartitions)
        ActivityWriter.cleanupPath(validOutputPath)

        val snapshotTargetDate = config.endDate
        val sessionSnapshot = SessionStateStore.buildSnapshot(sessionizedValid, snapshotTargetDate, config.runId)
        val sessionSnapshotPath =
          SessionStateStore.saveSnapshot(sessionSnapshot, config.sessionStateBasePath, snapshotTargetDate)

        BatchRunLogger.logStatus(
          runLogBasePath = config.runLogBasePath,
          runId = config.runId,
          targetDate = targetDate,
          status = BatchRunStatus.HiveRegistered,
          startedAt = startedAt,
          stagingPath = Some(validOutputPath),
          dlqPath = Some(dlqOutputPath),
          finalOutputPath = Some(finalOutputPath),
          processingStartDate = Some(config.startDate),
          processingEndDate = Some(config.endDate),
          snapshotSeedDate = Some(previousSnapshotDate),
          snapshotTargetDate = Some(config.endDate),
          uniqueSessionCount = Some(uniqueSessionCount),
          sessionSnapshotPath = Some(sessionSnapshotPath),
          outputPartitionCount = Some(outputPartitionCount),
          outputPartitionStart = outputPartitionStart,
          outputPartitionEnd = outputPartitionEnd
        )
        println(s"batch_status=${BatchRunStatus.HiveRegistered.entryName}")

        HiveTableManager.createActivityEventsTable(spark, config.hiveTableName, finalOutputPath)
        val registeredHivePartitions =
          HiveTableManager.addPartitions(spark, config.hiveTableName, finalOutputPath, outputPartitions)

        BatchRunLogger.logStatus(
          runLogBasePath = config.runLogBasePath,
          runId = config.runId,
          targetDate = targetDate,
          status = BatchRunStatus.WauCompleted,
          startedAt = startedAt,
          stagingPath = Some(validOutputPath),
          dlqPath = Some(dlqOutputPath),
          finalOutputPath = Some(finalOutputPath),
          processingStartDate = Some(config.startDate),
          processingEndDate = Some(config.endDate),
          snapshotSeedDate = Some(previousSnapshotDate),
          snapshotTargetDate = Some(config.endDate),
          uniqueSessionCount = Some(uniqueSessionCount),
          registeredHivePartitionsCount = Some(registeredHivePartitions.size),
          sessionSnapshotPath = Some(sessionSnapshotPath),
          message = Some(s"affected_week_start=$affectedWeekStart; affected_week_end=$affectedWeekEnd")
        )
        println(s"batch_status=${BatchRunStatus.WauCompleted.entryName}")

        val userWau = WauQueryExecutor.runUserWau(
          spark,
          config.hiveTableName,
          affectedWeekStart = Some(affectedWeekStart),
          affectedWeekEnd = Some(affectedWeekEnd)
        )
        val weeklyActiveSessions = WauQueryExecutor.runWeeklyActiveSessions(
          spark,
          config.hiveTableName,
          affectedWeekStart = Some(affectedWeekStart),
          affectedWeekEnd = Some(affectedWeekEnd)
        )

        WauQueryExecutor.writeResult(userWau, wauUsersOutputPath)
        WauQueryExecutor.writeResult(weeklyActiveSessions, weeklyActiveSessionsOutputPath)

        HiveTableManager.createWauUsersTable(spark, WauUsersTableName, wauUsersOutputPath)
        HiveTableManager.addWeekPartitions(spark, WauUsersTableName, wauUsersOutputPath, affectedWeeks)
        HiveTableManager.createWeeklyActiveSessionsTable(
          spark,
          WeeklyActiveSessionsTableName,
          weeklyActiveSessionsOutputPath
        )
        HiveTableManager.addWeekPartitions(
          spark,
          WeeklyActiveSessionsTableName,
          weeklyActiveSessionsOutputPath,
          affectedWeeks
        )

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
        println(s"wau_affected_week_range=$affectedWeekStart,$affectedWeekEnd")

        val wauSummary = WauExecutionSummary(
          wauUsersOutputPath = wauUsersOutputPath,
          weeklyActiveSessionsOutputPath = weeklyActiveSessionsOutputPath,
          wauUsersWeekCount = userWauWeekCount,
          wauUsersStartWeek = userWauStartWeek,
          wauUsersEndWeek = userWauEndWeek,
          weeklyActiveSessionsWeekCount = weeklyActiveSessionWeekCount,
          weeklyActiveSessionsStartWeek = weeklyActiveSessionsStartWeek,
          weeklyActiveSessionsEndWeek = weeklyActiveSessionsEndWeek
        )

        BatchRunLogger.logStatus(
          runLogBasePath = config.runLogBasePath,
          runId = config.runId,
          targetDate = targetDate,
          status = BatchRunStatus.Success,
          startedAt = startedAt,
          stagingPath = Some(validOutputPath),
          dlqPath = Some(dlqOutputPath),
          finalOutputPath = Some(finalOutputPath),
          processingStartDate = Some(config.startDate),
          processingEndDate = Some(config.endDate),
          snapshotSeedDate = Some(previousSnapshotDate),
          snapshotTargetDate = Some(config.endDate),
          qualityGateWarnings = qualityGateResult.warnings,
          inputRowCount = Some(inputCount),
          validRowCount = Some(validCount),
          invalidRowCount = Some(invalidCount),
          outputRowCount = Some(sessionizedCount),
          duplicateGroupCount = Some(duplicateGroupCount),
          duplicateRowsCount = Some(duplicateRowsCount),
          droppedDuplicateRowsCount = Some(droppedDuplicateRowsCount),
          dlqRatio = Some(dlqRatio),
          invalidReasonSummary = invalidReasonSummary,
          outputPartitionCount = Some(outputPartitionCount),
          outputPartitionStart = outputPartitionStart,
          outputPartitionEnd = outputPartitionEnd,
          uniqueSessionCount = Some(uniqueSessionCount),
          registeredHivePartitionsCount = Some(registeredHivePartitions.size),
          sessionSnapshotPath = Some(sessionSnapshotPath),
          message = Some(s"affected_week_start=$affectedWeekStart; affected_week_end=$affectedWeekEnd"),
          wauUsersOutputPath = Some(wauSummary.wauUsersOutputPath),
          weeklyActiveSessionsOutputPath = Some(wauSummary.weeklyActiveSessionsOutputPath),
          wauUsersWeekCount = Some(wauSummary.wauUsersWeekCount),
          wauUsersStartWeek = wauSummary.wauUsersStartWeek,
          wauUsersEndWeek = wauSummary.wauUsersEndWeek,
          weeklyActiveSessionsWeekCount = Some(wauSummary.weeklyActiveSessionsWeekCount),
          weeklyActiveSessionsStartWeek = wauSummary.weeklyActiveSessionsStartWeek,
          weeklyActiveSessionsEndWeek = wauSummary.weeklyActiveSessionsEndWeek
        )
        println(s"batch_status=${BatchRunStatus.Success.entryName}")

        println(s"mode=${config.mode.entryName}")
        println(s"run_id=${config.runId}")
        println(s"input_path=${config.inputPath}")
        println(s"valid_output_path=$validOutputPath")
        println(s"dlq_output_path=$dlqOutputPath")
        println(s"session_snapshot_path=$sessionSnapshotPath")
        println(s"registered_hive_partitions_count=${registeredHivePartitions.size}")
        println(s"final_output_path=$finalOutputPath")
        println(s"wau_users_output_path=$wauUsersOutputPath")
        println(s"weekly_active_sessions_output_path=$weeklyActiveSessionsOutputPath")
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
          invalid
            .groupBy("reject_reason")
            .count()
            .orderBy(col("count").desc, col("reject_reason"))
            .show(20, truncate = false)
        }

        if (qualityGateResult.warnings.nonEmpty) {
          println(s"quality_gate_warnings=${qualityGateResult.warnings.mkString("; ")}")
        }

        invalid.unpersist(blocking = false)
        duplicateGroups.unpersist(blocking = false)
        sessionizedValid.unpersist(blocking = false)
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
          finalOutputPath = Some(finalOutputPath),
          message = Some(error.getMessage),
          processingStartDate = Some(config.startDate),
          processingEndDate = Some(config.endDate),
          snapshotSeedDate = Some(previousSnapshotDate),
          snapshotTargetDate = Some(config.endDate)
        )
        println(s"batch_status=${BatchRunStatus.Failed.entryName}")
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
