package support

import model.BatchExecutionSummary

final case class QualityGateResult(warnings: Seq[String])

object QualityGate {
  private val WarningDlqRatio = 0.01d
  private val FailDlqRatio = 0.05d

  def evaluate(summary: BatchExecutionSummary): QualityGateResult = {
    require(summary.inputRowCount > 0L, "input_row_count must be greater than 0")
    require(summary.outputRowCount > 0L, "output_row_count must be greater than 0")

    val invalidEventTimeCount = summary.invalidReasonSummary.getOrElse("INVALID_EVENT_TIME", 0L)
    require(invalidEventTimeCount < summary.inputRowCount, "all rows failed event_time parsing")
    require(summary.dlqRatio <= FailDlqRatio, f"dlq_ratio must be <= $FailDlqRatio%.2f but was ${summary.dlqRatio}%.4f")

    val warnings =
      if (summary.dlqRatio > WarningDlqRatio) {
        Seq(f"dlq_ratio warning: ${summary.dlqRatio}%.4f")
      } else {
        Seq.empty
      }

    QualityGateResult(warnings = warnings)
  }
}
