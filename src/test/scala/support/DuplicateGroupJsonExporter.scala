package support

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

import java.nio.file.{Files, Paths}
import java.sql.{Date, Timestamp}
import java.util.{ArrayList, LinkedHashMap}

object DuplicateGroupJsonExporter {
  private val GroupedRowFieldNames = Seq(
    "duplicate_rank",
    "dedup_retained",
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "raw_user_session",
    "event_time_utc",
    "event_time_kst",
    "normalized_price",
    "event_date_kst"
  )

  def writeGroupedJson(duplicates: DataFrame, outputPath: String): Unit = {
    val groupedRows = sort_array(
      collect_list(
        struct(GroupedRowFieldNames.map(col): _*)
      )
    )

    val grouped = duplicates
      .groupBy("dedup_key")
      .agg(
        max(col("duplicate_group_size")).as("duplicate_group_size"),
        sum(when(not(col("dedup_retained")), lit(1)).otherwise(lit(0))).as("dropped_duplicate_row_count"),
        groupedRows.as("rows")
      )
      .orderBy(col("dedup_key"))

    val prettyGroups = new ArrayList[java.util.Map[String, AnyRef]]()

    grouped.collect().foreach { group =>
      val rows = group.getAs[Seq[Row]]("rows").map(rowToJavaMap)
      val retainedRow = rows.find(row => java.lang.Boolean.TRUE == row.get("dedup_retained")).orNull
      val droppedRows = new ArrayList[java.util.Map[String, AnyRef]]()

      rows.foreach { row =>
        if (java.lang.Boolean.FALSE == row.get("dedup_retained")) {
          droppedRows.add(row)
        }
      }

      val groupMap = new LinkedHashMap[String, AnyRef]()
      groupMap.put("dedup_key", group.getAs[String]("dedup_key"))
      groupMap.put("duplicate_group_size", java.lang.Long.valueOf(group.getAs[Long]("duplicate_group_size")))
      groupMap.put(
        "dropped_duplicate_row_count",
        java.lang.Long.valueOf(group.getAs[Long]("dropped_duplicate_row_count"))
      )
      groupMap.put("retained_row", retainedRow)
      groupMap.put("dropped_rows", droppedRows)

      prettyGroups.add(groupMap)
    }

    val outputFile = Paths.get(outputPath)
    Option(outputFile.getParent).foreach(parent => Files.createDirectories(parent))
    Files.deleteIfExists(outputFile)

    val mapper = new ObjectMapper()
    mapper.writerWithDefaultPrettyPrinter().writeValue(outputFile.toFile, prettyGroups)
  }

  private def rowToJavaMap(row: Row): java.util.Map[String, AnyRef] = {
    val map = new LinkedHashMap[String, AnyRef]()

    GroupedRowFieldNames.zipWithIndex.foreach { case (fieldName, index) =>
      map.put(fieldName, toJavaValue(row.get(index)))
    }

    map
  }

  private def toJavaValue(value: Any): AnyRef =
    value match {
      case null => null
      case boolean: java.lang.Boolean => boolean
      case int: java.lang.Integer => int
      case long: java.lang.Long => long
      case double: java.lang.Double => double
      case float: java.lang.Float => float
      case bigDecimal: java.math.BigDecimal => bigDecimal
      case timestamp: Timestamp => timestamp.toInstant.toString
      case date: Date => date.toString
      case other => other.toString
    }

  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "Usage: DuplicateGroupJsonExporter <duplicates-parquet-path> <json-output-file>")

    val inputPath = args(0)
    val outputPath = args(1)

    val spark = TestSparkSessionFactory.create("duplicate-group-json-exporter")

    try {
      val duplicates = spark.read.parquet(inputPath)
      writeGroupedJson(duplicates, outputPath)

      println(s"input_path=$inputPath")
      println(s"output_path=$outputPath")
    } finally {
      spark.stop()
    }
  }
}
