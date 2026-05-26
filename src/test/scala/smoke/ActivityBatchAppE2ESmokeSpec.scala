package smoke

import config.AppConfig
import logging.BatchRunLogger
import model.{BatchExecutionSummary, BatchMode, BatchRunStatus}
import org.apache.spark.sql.functions.{col, coalesce, length, lit, not}
import query.{HiveTableManager, WauQueryExecutor}
import reader.CsvActivityReader
import sessionization.{SessionStateStore, Sessionizer}
import support.{DuplicateGroupJsonExporter, EventDateRangeFilter, PathBuilder, PreflightValidator, QualityGate, SessionSnapshotJsonExporter, SparkFunSuite, TestHiveSparkSessionFactory}
import transform.{ActivityNormalizer, Deduplicator, Validator}
import writer.ActivityWriter

import java.time.Instant
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, Paths, SimpleFileVisitor}
import org.apache.spark.sql.SparkSession

class ActivityBatchAppE2ESmokeSpec extends SparkFunSuite {
  test("real 2019-Oct.csv sample should pass normalize validate dedup sessionize and parquet write flow") {
    val inputPath = resolveInputPath()
    assume(Files.exists(inputPath), s"Missing smoke input file: $inputPath")

    val sampleSize = resolveSampleSize()
    val runId = "oct_e2e_smoke"
    val persistentBaseDir = resolveOutputPath()
    val tempBaseDir = persistentBaseDir.getOrElse(Files.createTempDirectory("activity-oct-e2e-smoke-"))
    val validOutputPath = tempBaseDir.resolve("valid").toString
    val duplicateBasePath = tempBaseDir.resolve("duplicates")
    val duplicateOutputPath = duplicateBasePath.resolve("parquet").toString
    val duplicateGroupJsonOutputPath = tempBaseDir.resolve("duplicates").resolve("duplicate-groups.json").toString
    val sessionSnapshotBasePath = tempBaseDir.resolve("session-state").toString
    val sessionSnapshotJsonOutputPath = tempBaseDir.resolve("session-state").resolve("session-snapshot.json").toString
    val preflightStagingBasePath = tempBaseDir.resolve("staging").toString
    val preflightDlqBasePath = tempBaseDir.resolve("dlq").toString
    val preflightRunLogBasePath = tempBaseDir.resolve("batch-run-log").toString
    val batchRunLogPath = PathBuilder.batchRunLogPath(preflightRunLogBasePath, runId)
    val startedAt = Instant.now
    val targetDate = "2019-10-01"
    var runLogInitialized = false

    try {
      if (Files.exists(tempBaseDir)) {
        deleteRecursively(tempBaseDir)
      }
      Files.createDirectories(tempBaseDir)

      val preflightConfig = AppConfig(
        mode = BatchMode.Daily,
        startDate = "2019-10-01",
        endDate = "2019-10-01",
        inputPath = inputPath.toString,
        stagingBasePath = preflightStagingBasePath,
        dlqBasePath = preflightDlqBasePath,
        runLogBasePath = preflightRunLogBasePath,
        runId = runId
      )

      PreflightValidator.validate(preflightConfig)
      BatchRunLogger.logStatus(
        runLogBasePath = preflightRunLogBasePath,
        runId = runId,
        targetDate = targetDate,
        status = BatchRunStatus.Running,
        startedAt = startedAt,
        stagingPath = Some(validOutputPath),
        dlqPath = Some(duplicateOutputPath)
      )
      runLogInitialized = true

      val raw = CsvActivityReader.read(spark, inputPath.toString).limit(sampleSize).cache()
      val rawCount = raw.count()

      assert(rawCount > 0L)

      val normalized = ActivityNormalizer(raw, runId)
      val targetDateColumn = coalesce(col("event_date_kst").cast("string"), lit("2019-10-01"))
      val validationResult = Validator(normalized, targetDateColumn)
      val rangeFilteredValid = EventDateRangeFilter.filter(validationResult.valid, "2019-10-01", "2019-10-01")
      val deduplicationResult = Deduplicator.analyze(rangeFilteredValid)
      val deduplicated = deduplicationResult.deduplicated.cache()
      val sessionized = Sessionizer(deduplicated).cache()
      val duplicateGroups = deduplicationResult.duplicateGroups.cache()

      val validCount = rangeFilteredValid.count()
      val invalidCount = validationResult.invalid.count()
      val deduplicatedCount = deduplicated.count()
      val sessionizedCount = sessionized.count()
      val duplicateMetrics = duplicateGroups
        .agg(
          count(lit(1)).as("duplicate_group_count"),
          coalesce(sum(col("duplicate_group_size")), lit(0L)).cast("long").as("duplicate_rows_count"),
          coalesce(sum(col("dropped_duplicate_row_count")), lit(0L)).cast("long").as("dropped_duplicate_row_count")
        )
        .first()
      val duplicateGroupCount = duplicateMetrics.getAs[Long]("duplicate_group_count")
      val duplicateRowsCount = duplicateMetrics.getAs[Long]("duplicate_rows_count")
      val droppedDuplicateCount = duplicateMetrics.getAs[Long]("dropped_duplicate_row_count")
      val uniqueSessionCount = sessionized.select("session_id").distinct().count()
      val invalidReasonSummaryText =
        if (invalidCount > 0L) {
          validationResult.invalid
            .groupBy("reject_reason")
            .count()
            .orderBy(col("count").desc, col("reject_reason"))
            .collect()
            .map(row => s"${row.getString(0)}=${row.getLong(1)}")
            .mkString(", ")
        } else {
          "none"
        }

      assert(validCount + invalidCount === rawCount)
      assert(deduplicatedCount <= validCount)
      assert(sessionizedCount === deduplicatedCount)
      assert(droppedDuplicateCount === validCount - deduplicatedCount)
      assert(normalized.columns.contains("event_time_utc"))
      assert(normalized.columns.contains("event_time_kst"))
      assert(normalized.columns.contains("event_date_kst"))
      assert(deduplicated.columns.contains("dedup_key"))
      assert(sessionized.columns.contains("session_start_time_utc"))
      assert(sessionized.columns.contains("session_start_time_kst"))
      assert(sessionized.columns.contains("session_id"))
      assert(duplicateGroups.columns.contains("duplicate_group_size"))
      assert(duplicateGroups.columns.contains("dropped_duplicate_row_count"))

      val dedupKeyLengths = deduplicated.select(length(col("dedup_key")).as("dedup_key_length")).distinct().collect()
      assert(dedupKeyLengths.forall(_.getInt(0) == 64))

      ActivityWriter.writeToStaging(sessionized, validOutputPath)

      val written = spark.read.parquet(validOutputPath)
      assert(written.count() === sessionizedCount)
      assert(written.columns.contains("dedup_key"))
      assert(written.columns.contains("session_start_time_utc"))
      assert(written.columns.contains("session_start_time_kst"))
      assert(written.columns.contains("session_id"))
      assert(written.filter(col("event_time_utc").isNull).count() === 0L)
      assert(written.filter(col("session_id").isNull).count() === 0L)
      assert(written.select("event_date_kst").distinct().count() > 0L)
      val outputPartitions = written
        .select("event_date_kst")
        .distinct()
        .orderBy(col("event_date_kst"))
        .collect()
        .map(_.getDate(0).toString)
        .toSeq

      val summary = BatchExecutionSummary(
        inputRowCount = rawCount,
        validRowCount = validCount,
        invalidRowCount = invalidCount,
        outputRowCount = sessionizedCount,
        duplicateGroupCount = duplicateGroupCount,
        duplicateRowsCount = duplicateRowsCount,
        droppedDuplicateRowsCount = droppedDuplicateCount,
        dlqRatio = if (rawCount == 0L) 0.0d else invalidCount.toDouble / rawCount.toDouble,
        invalidReasonSummary =
          if (invalidCount > 0L) {
            validationResult.invalid
              .groupBy("reject_reason")
              .count()
              .collect()
              .map(row => row.getString(0) -> row.getLong(1))
              .toMap
          } else {
            Map.empty[String, Long]
          },
        outputPartitions = outputPartitions
      )

      val qualityGateResult = QualityGate.evaluate(summary)
      val qualityGateStatus =
        if (qualityGateResult.warnings.isEmpty) "PASS" else s"PASS_WITH_WARNING (${qualityGateResult.warnings.mkString("; ")})"
      val sessionSnapshot = SessionStateStore.buildSnapshot(sessionized, targetDate, runId)
      val sessionSnapshotPath = SessionStateStore.saveSnapshot(sessionSnapshot, sessionSnapshotBasePath, targetDate)
      SessionSnapshotJsonExporter.writePrettyJson(sessionSnapshot, sessionSnapshotJsonOutputPath)
      assert(Files.exists(Paths.get(sessionSnapshotJsonOutputPath)))

      BatchRunLogger.logStatus(
        runLogBasePath = preflightRunLogBasePath,
        runId = runId,
        targetDate = targetDate,
        status = BatchRunStatus.Validated,
        startedAt = startedAt,
        stagingPath = Some(validOutputPath),
        dlqPath = Some(duplicateOutputPath),
        inputRowCount = Some(rawCount),
        validRowCount = Some(validCount),
        invalidRowCount = Some(invalidCount),
        dlqRatio = Some(summary.dlqRatio),
        invalidReasonSummary = summary.invalidReasonSummary,
        message = if (qualityGateResult.warnings.nonEmpty) Some(qualityGateResult.warnings.mkString("; ")) else None
      )

      if (duplicateGroupCount > 0L) {
        duplicateGroups.write.mode("overwrite").parquet(duplicateOutputPath)
        DuplicateGroupJsonExporter.writeGroupedJson(duplicateGroups, duplicateGroupJsonOutputPath)

        val writtenDuplicates = spark.read.parquet(duplicateOutputPath)
        assert(writtenDuplicates.count() === duplicateGroupCount)
        assert(Files.exists(Paths.get(duplicateGroupJsonOutputPath)))
      } else {
        assert(!Files.exists(Paths.get(duplicateOutputPath)))
        assert(!Files.exists(Paths.get(duplicateGroupJsonOutputPath)))
      }

      BatchRunLogger.logStatus(
        runLogBasePath = preflightRunLogBasePath,
        runId = runId,
        targetDate = targetDate,
        status = BatchRunStatus.Success,
        startedAt = startedAt,
        stagingPath = Some(validOutputPath),
        dlqPath = Some(duplicateOutputPath),
        inputRowCount = Some(rawCount),
        validRowCount = Some(validCount),
        invalidRowCount = Some(invalidCount),
        outputRowCount = Some(sessionizedCount),
        duplicateGroupCount = Some(duplicateGroupCount),
        duplicateRowsCount = Some(duplicateRowsCount),
        droppedDuplicateRowsCount = Some(droppedDuplicateCount),
        dlqRatio = Some(summary.dlqRatio),
        invalidReasonSummary = summary.invalidReasonSummary,
        outputPartitionCount = Some(outputPartitions.size),
        outputPartitionStart = outputPartitions.headOption,
        outputPartitionEnd = outputPartitions.lastOption,
        message = if (qualityGateResult.warnings.nonEmpty) Some(qualityGateResult.warnings.mkString("; ")) else None
      )
      assert(Files.exists(Paths.get(batchRunLogPath)))

      info(s"Smoke status: preflight=PASS quality=$qualityGateStatus batch=SUCCESS")
      info(
        s"Smoke summary: sample=$sampleSize raw=$rawCount valid=$validCount invalid=$invalidCount " +
          s"deduplicated=$deduplicatedCount sessionized=$sessionizedCount unique_sessions=$uniqueSessionCount " +
          s"duplicate_groups=$duplicateGroupCount duplicate_rows=$duplicateRowsCount dropped_duplicates=$droppedDuplicateCount"
      )
      info(
        s"Smoke artifacts: partitions=${outputPartitions.mkString(", ")} valid=$validOutputPath " +
          s"batch_run_log=$batchRunLogPath session_snapshot_json=$sessionSnapshotJsonOutputPath " +
          s"duplicate_json=${if (duplicateGroupCount > 0L) duplicateGroupJsonOutputPath else "(not generated)"}"
      )
      if (invalidCount > 0L) {
        info(s"Smoke invalid reason summary: $invalidReasonSummaryText")
      }
    } catch {
      case error: Throwable =>
        if (runLogInitialized) {
          BatchRunLogger.logStatus(
            runLogBasePath = preflightRunLogBasePath,
            runId = runId,
            targetDate = targetDate,
            status = BatchRunStatus.Failed,
            startedAt = startedAt,
            stagingPath = Some(validOutputPath),
            dlqPath = Some(duplicateOutputPath),
            message = Some(Option(error.getMessage).getOrElse(error.getClass.getName))
          )
        }
        throw error
    } finally {
      if (persistentBaseDir.isEmpty) {
        deleteRecursively(tempBaseDir)
      }
    }
  }

  test("real 2019-Oct.csv sample should create and query Hive external table") {
    stopSparkSession()

    val inputPath = resolveHiveInputPath()
    assume(Files.exists(inputPath), s"Missing hive smoke input file: $inputPath")

    val sampleSize = resolveHiveSampleSize()
    val tableName = "activity_events_smoke"
    val persistentBaseDir = resolveHiveOutputPath()
    val tempBaseDir = persistentBaseDir.getOrElse(Files.createTempDirectory("activity-hive-e2e-smoke-"))
    val stagingOutputPath = tempBaseDir.resolve("staging-valid").toString
    val finalOutputPath = tempBaseDir.resolve("final-output").toString
    val wauOutputBasePath = tempBaseDir.resolve("wau-results").toString
    val wauUsersOutputPath = s"${wauOutputBasePath}/wau-users"
    val weeklyActiveSessionsOutputPath = s"${wauOutputBasePath}/weekly-active-sessions"
    val warehouseDir = tempBaseDir.resolve("warehouse")
    val metastoreDir = tempBaseDir.resolve("metastore_db")
    val sessionSnapshotBasePath = tempBaseDir.resolve("session-state").toString

    val hiveSpark = TestHiveSparkSessionFactory.create("activity-hive-e2e-smoke", warehouseDir, metastoreDir)

    try {
      val raw = CsvActivityReader.read(hiveSpark, inputPath.toString).limit(sampleSize).cache()
      val rawCount = raw.count()

      assert(rawCount > 0L)

      val normalized = ActivityNormalizer(raw, "hive_e2e_smoke")
      val targetDateColumn = coalesce(col("event_date_kst").cast("string"), lit("2019-10-01"))
      val validationResult = Validator(normalized, targetDateColumn)
      val rangeFilteredValid = EventDateRangeFilter.filter(validationResult.valid, "2019-10-01", "2019-10-01")
      val deduplicationResult = Deduplicator.analyze(rangeFilteredValid)
      val sessionized = Sessionizer(deduplicationResult.deduplicated).cache()
      val sessionSnapshot = SessionStateStore.buildSnapshot(sessionized, "2019-10-01", "hive_e2e_smoke")

      val sessionizedCount = sessionized.count()
      val invalidCount = validationResult.invalid.count()
      val outputPartitions = sessionized
        .select("event_date_kst")
        .distinct()
        .orderBy(col("event_date_kst"))
        .collect()
        .map(_.getDate(0).toString)
        .toSeq

      assert(sessionizedCount > 0L)

      ActivityWriter.writeToStaging(sessionized, stagingOutputPath)
      val promotedPaths = ActivityWriter.promoteToFinal(stagingOutputPath, finalOutputPath, outputPartitions)
      ActivityWriter.cleanupPath(stagingOutputPath)
      SessionStateStore.saveSnapshot(sessionSnapshot, sessionSnapshotBasePath, "2019-10-01")

      HiveTableManager.createActivityEventsTable(hiveSpark, tableName, finalOutputPath)
      val registeredPartitions = HiveTableManager.addPartitions(hiveSpark, tableName, finalOutputPath, outputPartitions)

      val userWau = WauQueryExecutor.runUserWau(hiveSpark, tableName)
      val weeklyActiveSessions = WauQueryExecutor.runWeeklyActiveSessions(hiveSpark, tableName)
      WauQueryExecutor.writeResult(userWau, wauUsersOutputPath)
      WauQueryExecutor.writeResult(weeklyActiveSessions, weeklyActiveSessionsOutputPath)
      val userWauRowCount = userWau.count()
      val weeklyActiveSessionRowCount = weeklyActiveSessions.count()

      assert(hiveSpark.catalog.tableExists(tableName))
      assert(promotedPaths.size === outputPartitions.size)
      assert(registeredPartitions.size === outputPartitions.size)

      val tableDf = hiveSpark.table(tableName)
      assert(tableDf.count() === sessionizedCount)
      assert(tableDf.columns.contains("session_id"))
      assert(tableDf.columns.contains("session_start_time_utc"))

      val shownPartitions = hiveSpark.sql(s"SHOW PARTITIONS $tableName").collect().map(_.getString(0)).toSeq
      assert(shownPartitions.size === outputPartitions.size)
      assert(userWauRowCount > 0L)
      assert(weeklyActiveSessionRowCount > 0L)
      assert(hiveSpark.read.parquet(wauUsersOutputPath).count() > 0L)
      assert(hiveSpark.read.parquet(weeklyActiveSessionsOutputPath).count() > 0L)

      info("Hive smoke status: SUCCESS")
      info(
        s"Hive smoke summary: sample=$sampleSize raw=$rawCount invalid=$invalidCount sessionized=$sessionizedCount " +
        s"table=$tableName partitions=${outputPartitions.mkString(", ")} promoted=${promotedPaths.size} " +
          s"wau_rows=$userWauRowCount weekly_session_rows=$weeklyActiveSessionRowCount"
      )
      info(
        s"Hive smoke artifacts: final_output=$finalOutputPath warehouse=$warehouseDir metastore=$metastoreDir " +
          s"registered_partitions=${registeredPartitions.size} wau_users=$wauUsersOutputPath weekly_active_sessions=$weeklyActiveSessionsOutputPath"
      )
    } finally {
      hiveSpark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()

      if (persistentBaseDir.isEmpty) {
        deleteRecursively(tempBaseDir)
      }
    }
  }

  private def resolveInputPath(): Path = {
    val defaultPath = Paths.get(sys.props.getOrElse("user.dir", ".")).resolve(".data/2019-Oct.csv")

    sys.props
      .get("smoke.input.path")
      .orElse(sys.env.get("SMOKE_INPUT_PATH"))
      .map(Paths.get(_))
      .getOrElse(defaultPath)
  }

  private def resolveSampleSize(): Int =
    sys.props
      .get("smoke.sample.limit")
      .orElse(sys.env.get("SMOKE_SAMPLE_LIMIT"))
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(_.toInt)
      .filter(_ > 0)
      .getOrElse(10000)

  private def resolveOutputPath(): Option[Path] =
    sys.props
      .get("smoke.output.path")
      .orElse(sys.env.get("SMOKE_OUTPUT_PATH"))
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(Paths.get(_))

  private def resolveHiveInputPath(): Path = {
    val defaultPath = Paths.get(sys.props.getOrElse("user.dir", ".")).resolve(".data/2019-Oct.csv")

    sys.props
      .get("hive.smoke.input.path")
      .orElse(sys.env.get("HIVE_SMOKE_INPUT_PATH"))
      .orElse(sys.props.get("smoke.input.path"))
      .orElse(sys.env.get("SMOKE_INPUT_PATH"))
      .map(Paths.get(_))
      .getOrElse(defaultPath)
  }

  private def resolveHiveSampleSize(): Int =
    resolveSampleSize()

  private def resolveHiveOutputPath(): Option[Path] =
    sys.props
      .get("hive.smoke.output.path")
      .orElse(sys.env.get("HIVE_SMOKE_OUTPUT_PATH"))
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(Paths.get(_))

  private def deleteRecursively(path: Path): Unit = {
    if (Files.notExists(path)) {
      ()
    } else {
      Files.walkFileTree(
        path,
        new SimpleFileVisitor[Path] {
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            Files.deleteIfExists(file)
            FileVisitResult.CONTINUE
          }

          override def postVisitDirectory(dir: Path, exc: java.io.IOException): FileVisitResult = {
            Files.deleteIfExists(dir)
            FileVisitResult.CONTINUE
          }
        }
      )
    }
  }
}
