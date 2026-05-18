package support

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, Row}

import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.util.{ArrayList, LinkedHashMap}

object SessionSnapshotJsonExporter {
  private val SnapshotFieldNames = Seq(
    "snapshot_date_kst",
    "user_id",
    "last_session_id",
    "last_session_start_time_utc",
    "last_event_time_utc",
    "last_event_time_kst",
    "updated_at",
    "run_id"
  )

  def writePrettyJson(snapshot: DataFrame, outputPath: String): Unit = {
    val rows = snapshot
      .orderBy("user_id")
      .collect()
      .map(rowToJavaMap)

    val payload = new ArrayList[java.util.Map[String, AnyRef]]()
    rows.foreach(payload.add)

    val outputFile = Paths.get(outputPath)
    Option(outputFile.getParent).foreach(parent => Files.createDirectories(parent))
    Files.deleteIfExists(outputFile)

    val mapper = new ObjectMapper()
    mapper.writerWithDefaultPrettyPrinter().writeValue(outputFile.toFile, payload)
  }

  private def rowToJavaMap(row: Row): java.util.Map[String, AnyRef] = {
    val map = new LinkedHashMap[String, AnyRef]()

    SnapshotFieldNames.zipWithIndex.foreach { case (fieldName, index) =>
      map.put(fieldName, toJavaValue(row.get(index)))
    }

    map
  }

  private def toJavaValue(value: Any): AnyRef =
    value match {
      case null => null
      case long: java.lang.Long => long
      case int: java.lang.Integer => int
      case timestamp: Timestamp => timestamp.toInstant.toString
      case other => other.toString
    }
}
