package model

sealed trait BatchRunStatus {
  def entryName: String
}

object BatchRunStatus {
  case object Running extends BatchRunStatus {
    override val entryName: String = "RUNNING"
  }

  case object Normalized extends BatchRunStatus {
    override val entryName: String = "NORMALIZING"
  }

  case object Validated extends BatchRunStatus {
    override val entryName: String = "VALIDATING"
  }

  case object Deduplicated extends BatchRunStatus {
    override val entryName: String = "DEDUPLICATING"
  }

  case object Sessionized extends BatchRunStatus {
    override val entryName: String = "SESSIONIZING"
  }

  case object Promoted extends BatchRunStatus {
    override val entryName: String = "PROMOTING"
  }

  case object HiveRegistered extends BatchRunStatus {
    override val entryName: String = "REGISTERING_HIVE"
  }

  case object WauCompleted extends BatchRunStatus {
    override val entryName: String = "COMPUTING_WAU"
  }

  case object Success extends BatchRunStatus {
    override val entryName: String = "SUCCESS"
  }

  case object Failed extends BatchRunStatus {
    override val entryName: String = "FAILED"
  }
}
