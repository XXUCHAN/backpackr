package writer

import org.apache.spark.sql.functions.col
import support.{PathBuilder, SparkFunSuite}

import java.nio.file.{Files, Path, Paths}

class ActivityWriterSpec extends SparkFunSuite {
  test("activity writer should promote staging partitions to final output and replace existing data") {
    val sparkSession = spark
    import sparkSession.implicits._

    val baseDir = Files.createTempDirectory("activity-writer-promote-")
    val stagingPath = baseDir.resolve("staging").toString
    val finalPath = baseDir.resolve("final").toString
    val partitionDate = "2019-10-01"

    val oldFinal = Seq(("old", partitionDate)).toDF("value", "event_date_kst")
    val newStaging = Seq(("new-1", partitionDate), ("new-2", partitionDate)).toDF("value", "event_date_kst")

    ActivityWriter.writeToStaging(oldFinal, finalPath)
    ActivityWriter.writeToStaging(newStaging, stagingPath)

    val promotedPaths = ActivityWriter.promoteToFinal(stagingPath, finalPath, Seq(partitionDate))

    val finalDf = sparkSession.read.parquet(finalPath)
    val finalValues = finalDf.select(col("value")).collect().map(_.getString(0)).toSet

    assert(promotedPaths === Seq(PathBuilder.partitionPath(finalPath, partitionDate)))
    assert(finalValues === Set("new-1", "new-2"))
    assert(Files.exists(Paths.get(PathBuilder.partitionPath(stagingPath, partitionDate))))
  }

  test("activity writer should retry promote and eventually succeed") {
    val sparkSession = spark
    import sparkSession.implicits._

    val baseDir = Files.createTempDirectory("activity-writer-retry-")
    val stagingPath = baseDir.resolve("staging").toString
    val finalPath = baseDir.resolve("final").toString
    val partitionDate = "2019-10-01"

    val oldFinal = Seq(("old", partitionDate)).toDF("value", "event_date_kst")
    val newStaging = Seq(("new-1", partitionDate), ("new-2", partitionDate)).toDF("value", "event_date_kst")

    ActivityWriter.writeToStaging(oldFinal, finalPath)
    ActivityWriter.writeToStaging(newStaging, stagingPath)

    val flakyOps = new FlakyPromoteFileOperations(failCount = 1)
    ActivityWriter.promoteToFinal(stagingPath, finalPath, Seq(partitionDate), flakyOps, maxRetryCount = 3)

    val finalDf = sparkSession.read.parquet(finalPath)
    val finalValues = finalDf.select(col("value")).collect().map(_.getString(0)).toSet

    assert(finalValues === Set("new-1", "new-2"))
    assert(flakyOps.failureCount === 1)
  }

  test("activity writer should rollback to previous final partition when promote keeps failing") {
    val sparkSession = spark
    import sparkSession.implicits._

    val baseDir = Files.createTempDirectory("activity-writer-rollback-")
    val stagingPath = baseDir.resolve("staging").toString
    val finalPath = baseDir.resolve("final").toString
    val partitionDate = "2019-10-01"

    val oldFinal = Seq(("old", partitionDate)).toDF("value", "event_date_kst")
    val newStaging = Seq(("new-1", partitionDate), ("new-2", partitionDate)).toDF("value", "event_date_kst")

    ActivityWriter.writeToStaging(oldFinal, finalPath)
    ActivityWriter.writeToStaging(newStaging, stagingPath)

    val alwaysFailingOps = new FlakyPromoteFileOperations(failCount = Int.MaxValue)

    val error = intercept[IllegalStateException] {
      ActivityWriter.promoteToFinal(stagingPath, finalPath, Seq(partitionDate), alwaysFailingOps, maxRetryCount = 2)
    }

    val finalDf = sparkSession.read.parquet(finalPath)
    val finalValues = finalDf.select(col("value")).collect().map(_.getString(0)).toSet

    assert(error.getMessage.contains("failed to promote partition after 2 attempt(s)"))
    assert(finalValues === Set("old"))
    assert(Files.exists(Paths.get(PathBuilder.partitionPath(stagingPath, partitionDate))))
  }

  private final class FlakyPromoteFileOperations(failCount: Int) extends FileOperations {
    var failureCount: Int = 0

    override def exists(path: Path): Boolean =
      DefaultFileOperations.exists(path)

    override def createDirectories(path: Path): Unit =
      DefaultFileOperations.createDirectories(path)

    override def move(source: Path, target: Path, atomic: Boolean): Unit = {
      if (
        source.toString.contains(".promote-work") &&
        target.getFileName != null &&
        target.getFileName.toString.startsWith("event_date_kst=") &&
        failureCount < failCount
      ) {
        failureCount += 1
        throw new java.io.IOException("simulated promote failure")
      }

      DefaultFileOperations.move(source, target, atomic)
    }

    override def deleteRecursively(path: Path): Unit =
      DefaultFileOperations.deleteRecursively(path)

    override def copyRecursively(source: Path, target: Path): Unit =
      DefaultFileOperations.copyRecursively(source, target)
  }
}
