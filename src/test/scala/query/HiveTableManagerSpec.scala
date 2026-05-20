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

  test("renderWauUsersDdl should replace table name and location") {
    val ddl = HiveTableManager.renderWauUsersDdl(
      tableName = "wau_users_by_week_test",
      location = "/tmp/wau-users-by-week-test"
    )

    assert(ddl.contains("CREATE EXTERNAL TABLE IF NOT EXISTS wau_users_by_week_test"))
    assert(ddl.contains("LOCATION '/tmp/wau-users-by-week-test'"))
    assert(!ddl.contains("LOCATION '/warehouse/wau_users_by_week/'"))
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

  test("buildAddWeekPartitionStatement should create week partition registration SQL") {
    val sql = HiveTableManager.buildAddWeekPartitionStatement(
      tableName = "wau_users_by_week_test",
      basePath = "/tmp/wau-users-by-week-test",
      partitionDate = "2019-09-30"
    )

    assert(sql.contains("ALTER TABLE wau_users_by_week_test"))
    assert(sql.contains("PARTITION (week_start_kst='2019-09-30')"))
    assert(sql.contains("LOCATION '/tmp/wau-users-by-week-test/week_start_kst=2019-09-30/'"))
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

  test("addWeekPartitions should register and refresh weekly aggregate partitions") {
    withHiveTestEnvironment("hive-week-partition-reregister") { (spark, tempBaseDir, projectRoot) =>
      val tableName = s"wau_users_weekly_${System.currentTimeMillis()}"
      val initialOutputDir = tempBaseDir.resolve("wau-initial")
      val updatedOutputDir = tempBaseDir.resolve("wau-updated")
      val relativeInitialOutputPath = projectRoot.relativize(initialOutputDir).toString
      val relativeUpdatedOutputPath = projectRoot.relativize(updatedOutputDir).toString

      writeWeeklyAggregateParquet(
        spark = spark,
        outputPath = initialOutputDir.toString,
        rows = Seq(("2019-09-30", 2L))
      )

      HiveTableManager.createWauUsersTable(spark, tableName, relativeInitialOutputPath)
      HiveTableManager.addWeekPartitions(spark, tableName, relativeInitialOutputPath, Seq("2019-09-30"))
      assert(spark.table(tableName).count() === 1L)

      writeWeeklyAggregateParquet(
        spark = spark,
        outputPath = updatedOutputDir.toString,
        rows = Seq(("2019-09-30", 3L), ("2019-10-07", 4L))
      )

      HiveTableManager.addWeekPartitions(
        spark,
        tableName,
        relativeUpdatedOutputPath,
        Seq("2019-09-30", "2019-10-07")
      )

      val countsByWeek = spark.table(tableName).collect().map(row => row.getDate(1).toString -> row.getLong(0)).toMap
      assert(countsByWeek("2019-09-30") === 3L)
      assert(countsByWeek("2019-10-07") === 4L)
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
           |date('2019-09-30') AS week_start_kst,
           |'dedup-$rowId' AS dedup_key,
           |'session-id-$rowId' AS session_id,
           |timestamp('2019-10-01 00:00:00') AS session_start_time_utc,
           |timestamp('2019-10-01 09:00:00') AS session_start_time_kst,
           |date('2019-09-30') AS session_start_week_kst,
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

  private def writeWeeklyAggregateParquet(
      spark: SparkSession,
      outputPath: String,
      rows: Seq[(String, Long)]
  ): Unit = {
    import spark.implicits._

    rows.toDF("week_start_kst", "wau_users")
      .withColumn("week_start_kst", $"week_start_kst".cast("date"))
      .write
      .mode("overwrite")
      .partitionBy("week_start_kst")
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
