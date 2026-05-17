import config.AppConfigParser
import org.apache.spark.sql.functions.{col, coalesce, lit, not}
import logging.BatchRunLogger
import model.{BatchExecutionSummary, BatchRunStatus}
import reader.CsvActivityReader
import support.{PathBuilder, PreflightValidator, QualityGate, SparkSessionFactory}
import transform.{ActivityNormalizer, Deduplicator, Validator}
import writer.{ActivityWriter, DlqWriter}

import java.time.Instant

object ActivityBatchApp {
  def main(args: Array[String]): Unit = {
    val config = AppConfigParser.parse(args)
    val startedAt = Instant.now
    val targetDate = config.startDate
    val validOutputPath = s"${PathBuilder.stagingRunPath(config.stagingBasePath, config.runId)}/valid"
    val dlqOutputPath = s"${PathBuilder.stagingRunPath(config.dlqBasePath, config.runId)}/invalid"
    val finalOutputPath = Option(config.outputBasePath).map(_.trim).filter(_.nonEmpty)

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
        finalOutputPath = finalOutputPath
      )

      val spark = SparkSessionFactory.create(config.appName)

      try {
        spark.sparkContext.setLogLevel("WARN")

        val raw = CsvActivityReader.read(spark, config.inputPath)
        val normalized = ActivityNormalizer(raw, config.runId)
        val targetDateColumn = coalesce(col("event_date_kst").cast("string"), lit(config.startDate))
        val validationResult = Validator(normalized, targetDateColumn)
        val deduplicationResult = Deduplicator.analyze(validationResult.valid)
        val deduplicatedValid = deduplicationResult.deduplicated
        val duplicates = deduplicationResult.duplicates

        ActivityWriter.writeToStaging(deduplicatedValid, validOutputPath)
        DlqWriter.write(validationResult.invalid, dlqOutputPath)

        val inputCount = raw.count()
        val validCount = validationResult.valid.count()
        val deduplicatedCount = deduplicatedValid.count()
        val invalidCount = validationResult.invalid.count()
        val duplicateRowsCount = duplicates.count()
        val droppedDuplicateRowsCount = duplicates.filter(not(col("dedup_retained"))).count()
        val duplicateGroupCount = if (duplicateRowsCount > 0L) duplicates.select("dedup_key").distinct().count() else 0L
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
        val outputPartitions = deduplicatedValid
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
          outputRowCount = deduplicatedCount,
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
            if (qualityGateResult.warnings.nonEmpty) Some(qualityGateResult.warnings.mkString("; ")) else None
        )

        finalOutputPath.foreach { outputPath =>
          ActivityWriter.writeToFinal(deduplicatedValid, outputPath)
          BatchRunLogger.logStatus(
            runLogBasePath = config.runLogBasePath,
            runId = config.runId,
            targetDate = targetDate,
            status = BatchRunStatus.Promoted,
            startedAt = startedAt,
            stagingPath = Some(validOutputPath),
            dlqPath = Some(dlqOutputPath),
            finalOutputPath = Some(outputPath),
            summary = Some(summary)
          )
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
            if (qualityGateResult.warnings.nonEmpty) Some(qualityGateResult.warnings.mkString("; ")) else None
        )

        println(s"mode=${config.mode.entryName}")
        println(s"run_id=${config.runId}")
        println(s"input_path=${config.inputPath}")
        println(s"valid_output_path=$validOutputPath")
        println(s"dlq_output_path=$dlqOutputPath")
        finalOutputPath.foreach(path => println(s"final_output_path=$path"))
        println(s"input_row_count=$inputCount")
        println(s"validated_row_count=$validCount")
        println(s"deduplicated_row_count=$deduplicatedCount")
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
          message = Some(error.getMessage)
        )
        throw error
    }
  }
}
