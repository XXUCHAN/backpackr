package logging

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import model.{BatchExecutionSummary, BatchRunStatus}
import support.PathBuilder

import java.nio.file.{Files, Paths}
import java.time.Instant

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
      summary: Option[BatchExecutionSummary] = None,
      processingStartDate: Option[String] = None,
      processingEndDate: Option[String] = None,
      snapshotSeedDate: Option[String] = None,
      snapshotTargetDate: Option[String] = None,
      qualityGateWarnings: Seq[String] = Seq.empty,
      uniqueSessionCount: Option[Long] = None,
      registeredHivePartitionsCount: Option[Int] = None,
      sessionSnapshotPath: Option[String] = None,
      wauUsersOutputPath: Option[String] = None,
      weeklyActiveSessionsOutputPath: Option[String] = None,
      wauUsersWeekCount: Option[Long] = None,
      wauUsersStartWeek: Option[String] = None,
      wauUsersEndWeek: Option[String] = None,
      weeklyActiveSessionsWeekCount: Option[Long] = None,
      weeklyActiveSessionsStartWeek: Option[String] = None,
      weeklyActiveSessionsEndWeek: Option[String] = None
  ): Unit = {
    val logPath = Paths.get(PathBuilder.batchRunLogPath(runLogBasePath, runId))
    Option(logPath.getParent).foreach(parent => Files.createDirectories(parent))

    val payload = objectMapper.createObjectNode()
    payload.put("run_id", runId)
    payload.put("target_date", targetDate)
    payload.put("status", status.entryName)
    payload.put("started_at", startedAt.toString)
    payload.put("updated_at", Instant.now.toString)

    message.foreach(value => payload.put("message", value))
    stagingPath.foreach(value => payload.put("staging_path", value))
    dlqPath.foreach(value => payload.put("dlq_path", value))
    finalOutputPath.foreach(value => payload.put("final_output_path", value))
    processingStartDate.foreach(value => payload.put("processing_start_date", value))
    processingEndDate.foreach(value => payload.put("processing_end_date", value))
    snapshotSeedDate.foreach(value => payload.put("snapshot_seed_date", value))
    snapshotTargetDate.foreach(value => payload.put("snapshot_target_date", value))
    uniqueSessionCount.foreach(value => payload.put("unique_session_count", value))
    registeredHivePartitionsCount.foreach(value => payload.put("registered_hive_partitions_count", value))
    sessionSnapshotPath.foreach(value => payload.put("session_snapshot_path", value))
    wauUsersOutputPath.foreach(value => payload.put("wau_users_output_path", value))
    weeklyActiveSessionsOutputPath.foreach(value => payload.put("weekly_active_sessions_output_path", value))
    wauUsersWeekCount.foreach(value => payload.put("wau_users_week_count", value))
    wauUsersStartWeek.foreach(value => payload.put("wau_users_start_week", value))
    wauUsersEndWeek.foreach(value => payload.put("wau_users_end_week", value))
    weeklyActiveSessionsWeekCount.foreach(value => payload.put("weekly_active_sessions_week_count", value))
    weeklyActiveSessionsStartWeek.foreach(value => payload.put("weekly_active_sessions_start_week", value))
    weeklyActiveSessionsEndWeek.foreach(value => payload.put("weekly_active_sessions_end_week", value))

    if (qualityGateWarnings.nonEmpty) {
      val warnings = objectMapper.createArrayNode()
      qualityGateWarnings.foreach(warnings.add)
      payload.replace("quality_gate_warnings", warnings)
    }

    summary.foreach { metrics =>
      payload.put("input_row_count", metrics.inputRowCount)
      payload.put("valid_row_count", metrics.validRowCount)
      payload.put("invalid_row_count", metrics.invalidRowCount)
      payload.put("output_row_count", metrics.outputRowCount)
      payload.put("duplicate_group_count", metrics.duplicateGroupCount)
      payload.put("duplicate_rows_count", metrics.duplicateRowsCount)
      payload.put("dropped_duplicate_rows_count", metrics.droppedDuplicateRowsCount)
      payload.put("dlq_ratio", metrics.dlqRatio)

      val outputPartitions = objectMapper.createArrayNode()
      metrics.outputPartitions.foreach(outputPartitions.add)
      payload.replace("output_partitions", outputPartitions)

      val invalidReasons = objectMapper.createObjectNode()
      metrics.invalidReasonSummary.toSeq.sortBy(_._1).foreach { case (reason, count) =>
        invalidReasons.put(reason, count)
      }
      payload.replace("invalid_reason_summary", invalidReasons)
    }

    val history = loadExistingHistory(logPath.toString)
    history.add(payload)
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(logPath.toFile, history)
  }

  private def loadExistingHistory(logPath: String): ArrayNode = {
    val path = Paths.get(logPath)

    if (Files.notExists(path)) {
      objectMapper.createArrayNode()
    } else {
      val existing = objectMapper.readTree(path.toFile)
      if (existing.isArray) {
        existing.deepCopy[ArrayNode]()
      } else {
        val array = objectMapper.createArrayNode()
        array.add(existing)
        array
      }
    }
  }
}
