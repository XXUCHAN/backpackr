import config.AppConfigParser
import org.apache.spark.sql.functions.{col, coalesce, lit}
import reader.CsvActivityReader
import support.PathBuilder
import support.SparkSessionFactory
import transform.{ActivityNormalizer, Deduplicator, Validator}
import writer.{ActivityWriter, DlqWriter}

object ActivityBatchApp {
  def main(args: Array[String]): Unit = {
    val config = AppConfigParser.parse(args)
    val spark = SparkSessionFactory.create(config.appName)

    try {
      spark.sparkContext.setLogLevel("WARN")

      val raw = CsvActivityReader.read(spark, config.inputPath)
      val normalized = ActivityNormalizer(raw, config.runId)
      val targetDateColumn = coalesce(col("event_date_kst").cast("string"), lit(config.startDate))
      val validationResult = Validator(normalized, targetDateColumn)
      val deduplicatedValid = Deduplicator(validationResult.valid)

      val validOutputPath = s"${PathBuilder.stagingRunPath(config.stagingBasePath, config.runId)}/valid"
      val dlqOutputPath = s"${PathBuilder.stagingRunPath(config.dlqBasePath, config.runId)}/invalid"

      ActivityWriter.writeToStaging(deduplicatedValid, validOutputPath)
      DlqWriter.write(validationResult.invalid, dlqOutputPath)

      val inputCount = raw.count()
      val validCount = validationResult.valid.count()
      val deduplicatedCount = deduplicatedValid.count()
      val invalidCount = validationResult.invalid.count()
      val duplicateCount = validCount - deduplicatedCount

      println(s"mode=${config.mode.entryName}")
      println(s"run_id=${config.runId}")
      println(s"input_path=${config.inputPath}")
      println(s"valid_output_path=$validOutputPath")
      println(s"dlq_output_path=$dlqOutputPath")
      println(s"input_row_count=$inputCount")
      println(s"validated_row_count=$validCount")
      println(s"deduplicated_row_count=$deduplicatedCount")
      println(s"duplicate_row_count=$duplicateCount")
      println(s"invalid_row_count=$invalidCount")

      if (invalidCount > 0) {
        println("invalid_reason_summary:")
        validationResult.invalid
          .groupBy("reject_reason")
          .count()
          .orderBy(col("count").desc, col("reject_reason"))
          .show(20, truncate = false)
      }
    } finally {
      spark.stop()
    }
  }
}
