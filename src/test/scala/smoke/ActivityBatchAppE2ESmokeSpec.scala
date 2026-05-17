package smoke

import org.apache.spark.sql.functions.{col, coalesce, length, lit, not}
import reader.CsvActivityReader
import support.{DuplicateGroupJsonExporter, SparkFunSuite}
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
    val duplicateOutputPath = tempBaseDir.resolve("duplicates").toString
    val duplicateGroupJsonOutputPath = tempBaseDir.resolve("duplicate-groups.json").toString

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
      val deduplicationResult = Deduplicator.analyze(validationResult.valid)
      val deduplicated = deduplicationResult.deduplicated.cache()
      val duplicates = deduplicationResult.duplicates.cache()

      val validCount = validationResult.valid.count()
      val invalidCount = validationResult.invalid.count()
      val deduplicatedCount = deduplicated.count()
      val duplicateRowsCount = duplicates.count()
      val droppedDuplicateCount = duplicates.filter(not(col("dedup_retained"))).count()
      val duplicateGroupCount =
        if (duplicateRowsCount > 0L) duplicates.select("dedup_key").distinct().count() else 0L
      val invalidReasonSummary =
        if (invalidCount > 0L) {
          validationResult.invalid
            .groupBy("reject_reason")
            .count()
            .orderBy(col("count").desc, col("reject_reason"))
            .collect()
            .map(row => s"${row.getString(0)}=${row.getLong(1)}")
            .mkString(", ")
        } else {
          "none"
        }

      assert(validCount + invalidCount === rawCount)
      assert(deduplicatedCount <= validCount)
      assert(droppedDuplicateCount === validCount - deduplicatedCount)
      assert(normalized.columns.contains("event_time_utc"))
      assert(normalized.columns.contains("event_time_kst"))
      assert(normalized.columns.contains("event_date_kst"))
      assert(deduplicated.columns.contains("dedup_key"))
      assert(duplicates.columns.contains("duplicate_group_size"))
      assert(duplicates.columns.contains("duplicate_rank"))
      assert(duplicates.columns.contains("dedup_retained"))

      val dedupKeyLengths = deduplicated.select(length(col("dedup_key")).as("dedup_key_length")).distinct().collect()
      assert(dedupKeyLengths.forall(_.getInt(0) == 64))

      ActivityWriter.writeToStaging(deduplicated, validOutputPath)

      val written = spark.read.parquet(validOutputPath)
      assert(written.count() === deduplicatedCount)
      assert(written.columns.contains("dedup_key"))
      assert(written.filter(col("event_time_utc").isNull).count() === 0L)
      assert(written.select("event_date_kst").distinct().count() > 0L)
      val outputPartitions = written
        .select("event_date_kst")
        .distinct()
        .orderBy(col("event_date_kst"))
        .collect()
        .map(_.getDate(0).toString)
        .mkString(", ")

      if (duplicateRowsCount > 0L) {
        ActivityWriter.writeToStaging(duplicates, duplicateOutputPath)
        DuplicateGroupJsonExporter.writeGroupedJson(duplicates, duplicateGroupJsonOutputPath)

        val writtenDuplicates = spark.read.parquet(duplicateOutputPath)
        assert(writtenDuplicates.count() === duplicateRowsCount)
        assert(Files.exists(Paths.get(duplicateGroupJsonOutputPath)))
      } else {
        assert(!Files.exists(Paths.get(duplicateOutputPath)))
        assert(!Files.exists(Paths.get(duplicateGroupJsonOutputPath)))
      }

      info(s"Smoke input path: $inputPath")
      info(s"Smoke sample limit requested: $sampleSize")
      info(s"Smoke raw rows read: $rawCount")
      info(s"Smoke validation passed rows: $validCount")
      info(s"Smoke validation failed rows: $invalidCount")
      info(s"Smoke invalid reason summary: $invalidReasonSummary")
      info(s"Smoke deduplicated output rows: $deduplicatedCount")
      info(s"Smoke duplicate group count: $duplicateGroupCount")
      info(s"Smoke output path: $validOutputPath")
      info(s"Smoke output partitions: $outputPartitions")
      info(
        s"Smoke duplicate output path: ${if (duplicateRowsCount > 0L) duplicateOutputPath else "(not generated)"}"
      )
      info(
        s"Smoke duplicate group json output path: ${if (duplicateRowsCount > 0L) duplicateGroupJsonOutputPath else "(not generated)"}"
      )
      info(s"Smoke duplicate rows: $duplicateRowsCount")
      info(s"Smoke dropped duplicate rows: $droppedDuplicateCount")
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
