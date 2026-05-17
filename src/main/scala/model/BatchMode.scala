package model

sealed trait BatchMode {
  def entryName: String
}

object BatchMode {
  case object Daily extends BatchMode {
    override val entryName: String = "daily"
  }

  case object Backfill extends BatchMode {
    override val entryName: String = "backfill"
  }

  def from(value: String): BatchMode =
    value.trim.toLowerCase match {
      case Daily.entryName    => Daily
      case Backfill.entryName => Backfill
      case other              => throw new IllegalArgumentException(s"Unsupported mode: $other")
    }
}
