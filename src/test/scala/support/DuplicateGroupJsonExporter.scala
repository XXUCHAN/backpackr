package support

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import java.nio.file.{Files, Paths}
import java.util.{ArrayList, LinkedHashMap}

object DuplicateGroupJsonExporter {
  def writeGroupedJson(duplicates: DataFrame, outputPath: String): Unit = {
    val grouped = duplicates
      .select("dedup_key", "duplicate_group_size", "dropped_duplicate_row_count")
      .orderBy(col("dedup_key"))

    val prettyGroups = new ArrayList[java.util.Map[String, AnyRef]]()

    grouped.collect().foreach { group =>
      val groupMap = new LinkedHashMap[String, AnyRef]()
      groupMap.put("dedup_key", group.getAs[String]("dedup_key"))
      groupMap.put("duplicate_group_size", java.lang.Long.valueOf(group.getAs[Long]("duplicate_group_size")))
      groupMap.put(
        "dropped_duplicate_row_count",
        java.lang.Long.valueOf(group.getAs[Long]("dropped_duplicate_row_count"))
      )

      prettyGroups.add(groupMap)
    }

    val outputFile = Paths.get(outputPath)
    Option(outputFile.getParent).foreach(parent => Files.createDirectories(parent))
    Files.deleteIfExists(outputFile)

    val mapper = new ObjectMapper()
    mapper.writerWithDefaultPrettyPrinter().writeValue(outputFile.toFile, prettyGroups)
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
