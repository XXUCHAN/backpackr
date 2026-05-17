package model

final case class BatchExecutionSummary(
    inputRowCount: Long,
    validRowCount: Long,
    invalidRowCount: Long,
    outputRowCount: Long,
    duplicateGroupCount: Long,
    duplicateRowsCount: Long,
    droppedDuplicateRowsCount: Long,
    dlqRatio: Double,
    invalidReasonSummary: Map[String, Long],
    outputPartitions: Seq[String]
)
