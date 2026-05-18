package writer

import org.apache.spark.sql.functions.col
import support.{PathBuilder, SparkFunSuite}

import java.nio.file.{Files, Paths}

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
    assert(Files.notExists(Paths.get(PathBuilder.partitionPath(stagingPath, partitionDate))))
  }
}
