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
      stagingPath = Some("/tmp/staging")
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
      summary = Some(summary)
    )

    val logPath = baseDir.resolve("run_id=run-1").resolve("batch-run-log.json")
    assert(Files.exists(logPath))

    val root = new com.fasterxml.jackson.databind.ObjectMapper().readTree(logPath.toFile)
    assert(root.isArray)
    assert(root.size() === 2)
    assert(root.get(0).get("status").asText() === "RUNNING")
    assert(root.get(1).get("status").asText() === "VALIDATED")
    assert(root.get(1).get("run_id").asText() === "run-1")
    assert(root.get(1).get("output_row_count").asLong() === 97L)
    assert(root.get(1).get("invalid_reason_summary").get("NULL_USER_ID").asLong() === 2L)
  }
}
