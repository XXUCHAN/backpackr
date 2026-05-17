package model

sealed trait BatchRunStatus {
  def entryName: String
}

object BatchRunStatus {
  case object Running extends BatchRunStatus {
    override val entryName: String = "RUNNING"
  }

  case object Validated extends BatchRunStatus {
    override val entryName: String = "VALIDATED"
  }

  case object Promoted extends BatchRunStatus {
    override val entryName: String = "PROMOTED"
  }

  case object Success extends BatchRunStatus {
    override val entryName: String = "SUCCESS"
  }

  case object Failed extends BatchRunStatus {
    override val entryName: String = "FAILED"
  }
}
