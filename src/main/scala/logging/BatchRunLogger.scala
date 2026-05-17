package logging

import com.fasterxml.jackson.databind.ObjectMapper
import model.{BatchExecutionSummary, BatchRunStatus}
import support.PathBuilder

import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.{ArrayList, LinkedHashMap}

object BatchRunLogger {
  private val objectMapper = new ObjectMapper()

  def logStatus(
      runLogBasePath: String,
      runId: String,
      targetDate: String,
      status: BatchRunStatus,
      startedAt: Instant,
      message: Option[String] = None,
      stagingPath: Option[String] = None,
      dlqPath: Option[String] = None,
      finalOutputPath: Option[String] = None,
      summary: Option[BatchExecutionSummary] = None
  ): Unit = {
    val logPath = Paths.get(PathBuilder.batchRunLogPath(runLogBasePath, runId))
    Option(logPath.getParent).foreach(parent => Files.createDirectories(parent))

    val payload = new LinkedHashMap[String, AnyRef]()
    payload.put("run_id", runId)
    payload.put("target_date", targetDate)
    payload.put("status", status.entryName)
    payload.put("started_at", startedAt.toString)
    payload.put("updated_at", Instant.now.toString)

    message.foreach(value => payload.put("message", value))
    stagingPath.foreach(value => payload.put("staging_path", value))
    dlqPath.foreach(value => payload.put("dlq_path", value))
    finalOutputPath.foreach(value => payload.put("final_output_path", value))

    summary.foreach { metrics =>
      payload.put("input_row_count", java.lang.Long.valueOf(metrics.inputRowCount))
      payload.put("valid_row_count", java.lang.Long.valueOf(metrics.validRowCount))
      payload.put("invalid_row_count", java.lang.Long.valueOf(metrics.invalidRowCount))
      payload.put("output_row_count", java.lang.Long.valueOf(metrics.outputRowCount))
      payload.put("duplicate_group_count", java.lang.Long.valueOf(metrics.duplicateGroupCount))
      payload.put("duplicate_rows_count", java.lang.Long.valueOf(metrics.duplicateRowsCount))
      payload.put("dropped_duplicate_rows_count", java.lang.Long.valueOf(metrics.droppedDuplicateRowsCount))
      payload.put("dlq_ratio", java.lang.Double.valueOf(metrics.dlqRatio))

      val outputPartitions = new ArrayList[String]()
      metrics.outputPartitions.foreach(outputPartitions.add)
      payload.put("output_partitions", outputPartitions)

      val invalidReasons = new LinkedHashMap[String, java.lang.Long]()
      metrics.invalidReasonSummary.toSeq.sortBy(_._1).foreach { case (reason, count) =>
        invalidReasons.put(reason, java.lang.Long.valueOf(count))
      }
      payload.put("invalid_reason_summary", invalidReasons)
    }

    objectMapper.writerWithDefaultPrettyPrinter().writeValue(logPath.toFile, payload)
  }
}
