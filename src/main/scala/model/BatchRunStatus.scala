package model

sealed trait BatchRunStatus {
  def entryName: String
}

object BatchRunStatus {
  case object Running extends BatchRunStatus {
    override val entryName: String = "RUNNING"
  }

  case object Normalized extends BatchRunStatus {
    override val entryName: String = "NORMALIZED"
  }

  case object Validated extends BatchRunStatus {
    override val entryName: String = "VALIDATED"
  }

  case object Deduplicated extends BatchRunStatus {
    override val entryName: String = "DEDUPLICATED"
  }

  case object Sessionized extends BatchRunStatus {
    override val entryName: String = "SESSIONIZED"
  }

  case object Promoted extends BatchRunStatus {
    override val entryName: String = "PROMOTED"
  }

  case object HiveRegistered extends BatchRunStatus {
    override val entryName: String = "HIVE_REGISTERED"
  }

  case object WauCompleted extends BatchRunStatus {
    override val entryName: String = "WAU_COMPLETED"
  }

  case object Success extends BatchRunStatus {
    override val entryName: String = "SUCCESS"
  }

  case object Failed extends BatchRunStatus {
    override val entryName: String = "FAILED"
  }
}
