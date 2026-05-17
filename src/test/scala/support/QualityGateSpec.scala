package support

import model.BatchExecutionSummary
import org.scalatest.funsuite.AnyFunSuite

class QualityGateSpec extends AnyFunSuite {
  test("quality gate should pass healthy batch metrics without warnings") {
    val summary = BatchExecutionSummary(
      inputRowCount = 1000L,
      validRowCount = 995L,
      invalidRowCount = 5L,
      outputRowCount = 990L,
      duplicateGroupCount = 3L,
      duplicateRowsCount = 8L,
      droppedDuplicateRowsCount = 5L,
      dlqRatio = 0.005d,
      invalidReasonSummary = Map("INVALID_EVENT_TIME" -> 5L),
      outputPartitions = Seq("2019-10-01")
    )

    val result = QualityGate.evaluate(summary)

    assert(result.warnings.isEmpty)
  }

  test("quality gate should emit warning when dlq ratio is above warning threshold") {
    val summary = BatchExecutionSummary(
      inputRowCount = 1000L,
      validRowCount = 985L,
      invalidRowCount = 15L,
      outputRowCount = 980L,
      duplicateGroupCount = 2L,
      duplicateRowsCount = 7L,
      droppedDuplicateRowsCount = 5L,
      dlqRatio = 0.015d,
      invalidReasonSummary = Map("INVALID_EVENT_TIME" -> 15L),
      outputPartitions = Seq("2019-10-01")
    )

    val result = QualityGate.evaluate(summary)

    assert(result.warnings.exists(_.contains("dlq_ratio warning")))
  }

  test("quality gate should fail when output row count is zero") {
    val summary = BatchExecutionSummary(
      inputRowCount = 1000L,
      validRowCount = 1000L,
      invalidRowCount = 0L,
      outputRowCount = 0L,
      duplicateGroupCount = 0L,
      duplicateRowsCount = 0L,
      droppedDuplicateRowsCount = 0L,
      dlqRatio = 0.0d,
      invalidReasonSummary = Map.empty,
      outputPartitions = Seq.empty
    )

    val error = intercept[IllegalArgumentException] {
      QualityGate.evaluate(summary)
    }

    assert(error.getMessage.contains("output_row_count"))
  }
}
