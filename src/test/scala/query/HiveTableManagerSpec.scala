package query

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import support.TestHiveSparkSessionFactory

import java.nio.file.{Files, Path, Paths}

class HiveTableManagerSpec extends AnyFunSuite {
  test("renderActivityEventsDdl should replace table name and location") {
    val ddl = HiveTableManager.renderActivityEventsDdl(
      tableName = "activity_events_test",
      location = "/tmp/activity-events-test"
    )

    assert(ddl.contains("CREATE EXTERNAL TABLE IF NOT EXISTS activity_events_test"))
    assert(ddl.contains("LOCATION '/tmp/activity-events-test'"))
    assert(!ddl.contains("LOCATION '/warehouse/activity_events/'"))
  }

  test("buildAddPartitionStatement should create partition registration SQL") {
    val sql = HiveTableManager.buildAddPartitionStatement(
      tableName = "activity_events_test",
      basePath = "/tmp/activity-events-test",
      partitionDate = "2019-10-01"
    )

    assert(sql.contains("ALTER TABLE activity_events_test"))
    assert(sql.contains("PARTITION (event_date_kst='2019-10-01')"))
    assert(sql.contains("LOCATION '/tmp/activity-events-test/event_date_kst=2019-10-01/'"))
  }

  test("createActivityEventsTable should resolve relative locations so table can read parquet data") {
    withHiveTestEnvironment("hive-relative-location") { (spark, tempBaseDir, projectRoot) =>
      val tableName = s"activity_events_relative_${System.currentTimeMillis()}"
      val finalOutputDir = tempBaseDir.resolve("final-output")
      val relativeFinalOutputPath = projectRoot.relativize(finalOutputDir).toString

      writeActivityEventsParquet(
        spark = spark,
        outputPath = finalOutputDir.toString,
        partitionDate = "2019-10-01",
        rowIds = Seq(1L, 2L)
      )

      HiveTableManager.createActivityEventsTable(spark, tableName, relativeFinalOutputPath)
      HiveTableManager.addPartitions(spark, tableName, relativeFinalOutputPath, Seq("2019-10-01"))

      assert(spark.table(tableName).count() === 2L)
    }
  }

  test("addPartitions should refresh existing partition locations on re-registration") {
    withHiveTestEnvironment("hive-partition-reregister") { (spark, tempBaseDir, projectRoot) =>
      val tableName = s"activity_events_reregister_${System.currentTimeMillis()}"
      val initialOutputDir = tempBaseDir.resolve("final-output-initial")
      val updatedOutputDir = tempBaseDir.resolve("final-output-updated")
      val relativeInitialOutputPath = projectRoot.relativize(initialOutputDir).toString
      val relativeUpdatedOutputPath = projectRoot.relativize(updatedOutputDir).toString

      writeActivityEventsParquet(
        spark = spark,
        outputPath = initialOutputDir.toString,
        partitionDate = "2019-10-01",
        rowIds = Seq(1L)
      )

      HiveTableManager.createActivityEventsTable(spark, tableName, relativeInitialOutputPath)
      HiveTableManager.addPartitions(spark, tableName, relativeInitialOutputPath, Seq("2019-10-01"))
      assert(spark.table(tableName).count() === 1L)

      writeActivityEventsParquet(
        spark = spark,
        outputPath = updatedOutputDir.toString,
        partitionDate = "2019-10-01",
        rowIds = Seq(10L, 11L)
      )

      HiveTableManager.addPartitions(spark, tableName, relativeUpdatedOutputPath, Seq("2019-10-01"))

      assert(spark.table(tableName).count() === 2L)
    }
  }

  private def withHiveTestEnvironment(testName: String)(testCode: (SparkSession, Path, Path) => Unit): Unit = {
    val projectRoot = Paths.get(sys.props.getOrElse("user.dir", ".")).toAbsolutePath.normalize
    val tempRoot = projectRoot.resolve(".tmp").resolve("tests")
    Files.createDirectories(tempRoot)

    val tempBaseDir = Files.createTempDirectory(tempRoot, s"$testName-")
    val warehouseDir = tempBaseDir.resolve("warehouse")
    val metastoreDir = tempBaseDir.resolve("metastore_db")
    val spark = TestHiveSparkSessionFactory.create(testName, warehouseDir, metastoreDir)

    try {
      testCode(spark, tempBaseDir, projectRoot)
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      deleteRecursively(tempBaseDir)
    }
  }

  private def writeActivityEventsParquet(
      spark: SparkSession,
      outputPath: String,
      partitionDate: String,
      rowIds: Seq[Long]
  ): Unit = {
    val rowsSql = rowIds
      .map { rowId =>
        s"""SELECT
           |timestamp('2019-10-01 00:00:00') AS event_time_utc,
           |timestamp('2019-10-01 09:00:00') AS event_time_kst,
           |'view' AS event_type,
           |cast(${1000L + rowId} as bigint) AS product_id,
           |cast(${2000L + rowId} as bigint) AS category_id,
           |'electronics.audio' AS category_code,
           |'sony' AS brand,
           |cast(10.50 as decimal(18,2)) AS price,
           |cast(${3000L + rowId} as bigint) AS user_id,
           |'session-$rowId' AS raw_user_session,
           |'dedup-$rowId' AS dedup_key,
           |'session-id-$rowId' AS session_id,
           |timestamp('2019-10-01 00:00:00') AS session_start_time_utc,
           |timestamp('2019-10-01 09:00:00') AS session_start_time_kst,
           |timestamp('2019-10-01 00:10:00') AS ingested_at,
           |'run-$rowId' AS run_id,
           |date('$partitionDate') AS event_date_kst""".stripMargin
      }
      .mkString(" UNION ALL ")

    spark.sql(rowsSql)
      .write
      .mode("overwrite")
      .partitionBy("event_date_kst")
      .parquet(outputPath)
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.notExists(path)) {
      ()
    } else {
      Files.walk(path).sorted(java.util.Comparator.reverseOrder()).forEach(file => Files.deleteIfExists(file))
    }
  }
}
