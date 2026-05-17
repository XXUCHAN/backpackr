package smoke

import org.apache.spark.sql.functions.{col, coalesce, length, lit}
import reader.CsvActivityReader
import support.SparkFunSuite
import transform.{ActivityNormalizer, Deduplicator, Validator}
import writer.ActivityWriter

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, Paths, SimpleFileVisitor}

class ActivityBatchAppE2ESmokeSpec extends SparkFunSuite {
  test("real 2019-Oct.csv sample should pass normalize validate dedup and parquet write flow") {
    val inputPath = resolveInputPath()
    assume(Files.exists(inputPath), s"Missing smoke input file: $inputPath")

    val sampleSize = resolveSampleSize()
    val runId = "oct_e2e_smoke"
    val persistentBaseDir = resolveOutputPath()
    val tempBaseDir = persistentBaseDir.getOrElse(Files.createTempDirectory("activity-oct-e2e-smoke-"))
    val validOutputPath = tempBaseDir.resolve("valid").toString

    try {
      if (Files.exists(tempBaseDir)) {
        deleteRecursively(tempBaseDir)
      }
      Files.createDirectories(tempBaseDir)

      val raw = CsvActivityReader.read(spark, inputPath.toString).limit(sampleSize).cache()
      val rawCount = raw.count()

      assert(rawCount > 0L)

      val normalized = ActivityNormalizer(raw, runId)
      val targetDateColumn = coalesce(col("event_date_kst").cast("string"), lit("2019-10-01"))
      val validationResult = Validator(normalized, targetDateColumn)
      val deduplicated = Deduplicator(validationResult.valid).cache()

      val validCount = validationResult.valid.count()
      val invalidCount = validationResult.invalid.count()
      val deduplicatedCount = deduplicated.count()

      assert(validCount + invalidCount === rawCount)
      assert(deduplicatedCount <= validCount)
      assert(normalized.columns.contains("event_time_utc"))
      assert(normalized.columns.contains("event_time_kst"))
      assert(normalized.columns.contains("event_date_kst"))
      assert(deduplicated.columns.contains("dedup_key"))

      val dedupKeyLengths = deduplicated.select(length(col("dedup_key")).as("dedup_key_length")).distinct().collect()
      assert(dedupKeyLengths.forall(_.getInt(0) == 64))

      ActivityWriter.writeToStaging(deduplicated, validOutputPath)

      val written = spark.read.parquet(validOutputPath)
      assert(written.count() === deduplicatedCount)
      assert(written.columns.contains("dedup_key"))
      assert(written.filter(col("event_time_utc").isNull).count() === 0L)
      assert(written.select("event_date_kst").distinct().count() > 0L)
      info(s"Smoke output path: $validOutputPath")
    } finally {
      if (persistentBaseDir.isEmpty) {
        deleteRecursively(tempBaseDir)
      }
    }
  }

  private def resolveInputPath(): Path = {
    val defaultPath = Paths.get(sys.props.getOrElse("user.dir", ".")).resolve(".data/2019-Oct.csv")

    sys.props
      .get("smoke.input.path")
      .orElse(sys.env.get("SMOKE_INPUT_PATH"))
      .map(Paths.get(_))
      .getOrElse(defaultPath)
  }

  private def resolveSampleSize(): Int =
    sys.props
      .get("smoke.sample.limit")
      .orElse(sys.env.get("SMOKE_SAMPLE_LIMIT"))
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(_.toInt)
      .filter(_ > 0)
      .getOrElse(10000)

  private def resolveOutputPath(): Option[Path] =
    sys.props
      .get("smoke.output.path")
      .orElse(sys.env.get("SMOKE_OUTPUT_PATH"))
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(Paths.get(_))

  private def deleteRecursively(path: Path): Unit = {
    if (Files.notExists(path)) {
      ()
    } else {
      Files.walkFileTree(
        path,
        new SimpleFileVisitor[Path] {
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            Files.deleteIfExists(file)
            FileVisitResult.CONTINUE
          }

          override def postVisitDirectory(dir: Path, exc: java.io.IOException): FileVisitResult = {
            Files.deleteIfExists(dir)
            FileVisitResult.CONTINUE
          }
        }
      )
    }
  }
}
