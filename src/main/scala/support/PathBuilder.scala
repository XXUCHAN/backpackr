package support

object PathBuilder {
  def partitionPath(basePath: String, eventDateKst: String): String =
    s"$basePath/event_date_kst=$eventDateKst"

  def stagingRunPath(basePath: String, runId: String): String =
    s"$basePath/run_id=$runId"

  def batchRunLogPath(basePath: String, runId: String): String =
    s"$basePath/run_id=$runId/batch-run-log.json"
}
