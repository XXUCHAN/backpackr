package support

import logging.BatchRunLogger
import model.BatchRunStatus
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files
import java.time.Instant

class BatchRunLoggerSpec extends AnyFunSuite {
  test("batch run logger should append status history as pretty json array") {
    val baseDir = Files.createTempDirectory("batch-run-log-spec-")
    val startedAt = Instant.parse("2026-05-18T00:00:00Z")

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
      stagingPath = Some("/tmp/staging"),
      dlqPath = Some("/tmp/dlq"),
      finalOutputPath = Some("/tmp/final"),
      processingStartDate = Some("2019-10-01"),
      processingEndDate = Some("2019-10-15"),
      snapshotSeedDate = Some("2019-09-30"),
      snapshotTargetDate = Some("2019-10-15"),
      inputRowCount = Some(100L),
      validRowCount = Some(98L),
      invalidRowCount = Some(2L),
      dlqRatio = Some(0.02d),
      invalidReasonSummary = Map("NULL_USER_ID" -> 2L),
      qualityGateWarnings = Seq("dlq_ratio > 0.01"),
      outputPartitionCount = Some(15),
      outputPartitionStart = Some("2019-10-01"),
      outputPartitionEnd = Some("2019-10-15")
    )

    val logPath = baseDir.resolve("run_id=run-1").resolve("batch-run-log.json")
    assert(Files.exists(logPath))

    val root = new com.fasterxml.jackson.databind.ObjectMapper().readTree(logPath.toFile)
    assert(root.isArray)
    assert(root.size() === 2)
    assert(root.get(0).get("status").asText() === "RUNNING")
    assert(root.get(1).get("status").asText() === "VALIDATING")
    assert(root.get(1).get("run_id").asText() === "run-1")
    assert(root.get(1).get("processing_start_date").asText() === "2019-10-01")
    assert(root.get(1).get("processing_end_date").asText() === "2019-10-15")
    assert(root.get(1).get("snapshot_seed_date").asText() === "2019-09-30")
    assert(root.get(1).get("snapshot_target_date").asText() === "2019-10-15")
    assert(root.get(1).get("input_row_count").asLong() === 100L)
    assert(root.get(1).get("valid_row_count").asLong() === 98L)
    assert(root.get(1).get("invalid_row_count").asLong() === 2L)
    assert(root.get(1).get("dlq_ratio").asDouble() === 0.02d)
    assert(root.get(1).get("quality_gate_warnings").size() === 1)
    assert(root.get(1).get("quality_gate_warnings").get(0).asText() === "dlq_ratio > 0.01")
    assert(root.get(1).get("output_partition_count").asInt() === 15)
    assert(root.get(1).get("output_partition_start").asText() === "2019-10-01")
    assert(root.get(1).get("output_partition_end").asText() === "2019-10-15")
    assert(root.get(1).get("invalid_reason_summary").get("NULL_USER_ID").asLong() === 2L)
    assert(root.get(1).get("output_partitions") === null)
  }
}
