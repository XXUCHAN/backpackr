package support

import logging.BatchRunLogger
import model.{BatchExecutionSummary, BatchRunStatus}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files
import java.time.Instant

class BatchRunLoggerSpec extends AnyFunSuite {
  test("batch run logger should append status history as pretty json array") {
    val baseDir = Files.createTempDirectory("batch-run-log-spec-")
    val startedAt = Instant.parse("2026-05-18T00:00:00Z")

    val summary = BatchExecutionSummary(
      inputRowCount = 100L,
      validRowCount = 98L,
      invalidRowCount = 2L,
      outputRowCount = 97L,
      duplicateGroupCount = 1L,
      duplicateRowsCount = 2L,
      droppedDuplicateRowsCount = 1L,
      dlqRatio = 0.02d,
      invalidReasonSummary = Map("NULL_USER_ID" -> 2L),
      outputPartitions = Seq("2019-10-01")
    )

    BatchRunLogger.logStatus(
      runLogBasePath = baseDir.toString,
      runId = "run-1",
      targetDate = "2019-10-01",
      status = BatchRunStatus.Running,
      startedAt = startedAt,
      message = Some("started"),
      stagingPath = Some("/tmp/staging"),
      processingStartDate = Some("2019-10-01"),
      processingEndDate = Some("2019-10-15"),
      snapshotSeedDate = Some("2019-09-30"),
      snapshotTargetDate = Some("2019-10-15")
    )

    BatchRunLogger.logStatus(
      runLogBasePath = baseDir.toString,
      runId = "run-1",
      targetDate = "2019-10-01",
      status = BatchRunStatus.Validated,
      startedAt = startedAt,
      message = Some("dlq_ratio warning"),
      stagingPath = Some("/tmp/staging"),
      dlqPath = Some("/tmp/dlq"),
      finalOutputPath = Some("/tmp/final"),
      summary = Some(summary),
      processingStartDate = Some("2019-10-01"),
      processingEndDate = Some("2019-10-15"),
      snapshotSeedDate = Some("2019-09-30"),
      snapshotTargetDate = Some("2019-10-15"),
      qualityGateWarnings = Seq("dlq_ratio > 0.01"),
      uniqueSessionCount = Some(88L),
      registeredHivePartitionsCount = Some(15),
      sessionSnapshotPath = Some("/tmp/session-state/snapshot_date_kst=2019-10-15"),
      wauUsersOutputPath = Some("/tmp/wau-users"),
      weeklyActiveSessionsOutputPath = Some("/tmp/weekly-active-sessions"),
      wauUsersWeekCount = Some(3L),
      wauUsersStartWeek = Some("2019-09-30"),
      wauUsersEndWeek = Some("2019-10-14"),
      weeklyActiveSessionsWeekCount = Some(3L),
      weeklyActiveSessionsStartWeek = Some("2019-09-30"),
      weeklyActiveSessionsEndWeek = Some("2019-10-14")
    )

    val logPath = baseDir.resolve("run_id=run-1").resolve("batch-run-log.json")
    assert(Files.exists(logPath))

    val root = new com.fasterxml.jackson.databind.ObjectMapper().readTree(logPath.toFile)
    assert(root.isArray)
    assert(root.size() === 2)
    assert(root.get(0).get("status").asText() === "RUNNING")
    assert(root.get(1).get("status").asText() === "VALIDATED")
    assert(root.get(1).get("run_id").asText() === "run-1")
    assert(root.get(1).get("processing_start_date").asText() === "2019-10-01")
    assert(root.get(1).get("processing_end_date").asText() === "2019-10-15")
    assert(root.get(1).get("snapshot_seed_date").asText() === "2019-09-30")
    assert(root.get(1).get("snapshot_target_date").asText() === "2019-10-15")
    assert(root.get(1).get("quality_gate_warnings").size() === 1)
    assert(root.get(1).get("quality_gate_warnings").get(0).asText() === "dlq_ratio > 0.01")
    assert(root.get(1).get("unique_session_count").asLong() === 88L)
    assert(root.get(1).get("registered_hive_partitions_count").asInt() === 15)
    assert(root.get(1).get("session_snapshot_path").asText() === "/tmp/session-state/snapshot_date_kst=2019-10-15")
    assert(root.get(1).get("wau_users_output_path").asText() === "/tmp/wau-users")
    assert(root.get(1).get("weekly_active_sessions_output_path").asText() === "/tmp/weekly-active-sessions")
    assert(root.get(1).get("wau_users_week_count").asLong() === 3L)
    assert(root.get(1).get("wau_users_start_week").asText() === "2019-09-30")
    assert(root.get(1).get("wau_users_end_week").asText() === "2019-10-14")
    assert(root.get(1).get("weekly_active_sessions_week_count").asLong() === 3L)
    assert(root.get(1).get("weekly_active_sessions_start_week").asText() === "2019-09-30")
    assert(root.get(1).get("weekly_active_sessions_end_week").asText() === "2019-10-14")
    assert(root.get(1).get("output_row_count").asLong() === 97L)
    assert(root.get(1).get("invalid_reason_summary").get("NULL_USER_ID").asLong() === 2L)
  }
}
