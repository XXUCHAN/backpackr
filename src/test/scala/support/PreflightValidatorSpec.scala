package support

import config.AppConfig
import model.BatchMode
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files
import java.time.LocalDate

class PreflightValidatorSpec extends AnyFunSuite {
  test("preflight validator should pass for existing input path and clean run paths") {
    val baseDir = Files.createTempDirectory("preflight-pass-")
    val inputFile = Files.createTempFile(baseDir, "input", ".csv")

    val config = AppConfig(
      mode = BatchMode.Daily,
      startDate = "2019-10-01",
      endDate = "2019-10-31",
      inputPath = inputFile.toString,
      stagingBasePath = baseDir.resolve("staging").toString,
      dlqBasePath = baseDir.resolve("dlq").toString,
      runLogBasePath = baseDir.resolve("run-log").toString,
      runId = "run-1"
    )

    PreflightValidator.validate(config)
  }

  test("preflight validator should fail when input path does not exist") {
    val baseDir = Files.createTempDirectory("preflight-missing-input-")

    val config = AppConfig(
      mode = BatchMode.Daily,
      startDate = "2019-10-01",
      endDate = "2019-10-31",
      inputPath = baseDir.resolve("missing.csv").toString,
      stagingBasePath = baseDir.resolve("staging").toString,
      dlqBasePath = baseDir.resolve("dlq").toString,
      runLogBasePath = baseDir.resolve("run-log").toString,
      runId = "run-1"
    )

    val error = intercept[IllegalArgumentException] {
      PreflightValidator.validate(config)
    }

    assert(error.getMessage.contains("input-path does not exist"))
  }

  test("preflight validator should fail for future end-date") {
    val baseDir = Files.createTempDirectory("preflight-future-date-")
    val inputFile = Files.createTempFile(baseDir, "input", ".csv")
    val futureDate = LocalDate.now().plusDays(1).toString

    val config = AppConfig(
      mode = BatchMode.Daily,
      startDate = "2019-10-01",
      endDate = futureDate,
      inputPath = inputFile.toString,
      stagingBasePath = baseDir.resolve("staging").toString,
      dlqBasePath = baseDir.resolve("dlq").toString,
      runLogBasePath = baseDir.resolve("run-log").toString,
      runId = "run-1"
    )

    val error = intercept[IllegalArgumentException] {
      PreflightValidator.validate(config)
    }

    assert(error.getMessage.contains("end-date must not be in the future"))
  }

  test("preflight validator should fail when staging path for the same run_id already exists") {
    val baseDir = Files.createTempDirectory("preflight-existing-staging-")
    val inputFile = Files.createTempFile(baseDir, "input", ".csv")
    val stagingRunPath = baseDir.resolve("staging").resolve("run_id=run-1")
    Files.createDirectories(stagingRunPath)

    val config = AppConfig(
      mode = BatchMode.Daily,
      startDate = "2019-10-01",
      endDate = "2019-10-31",
      inputPath = inputFile.toString,
      stagingBasePath = baseDir.resolve("staging").toString,
      dlqBasePath = baseDir.resolve("dlq").toString,
      runLogBasePath = baseDir.resolve("run-log").toString,
      runId = "run-1"
    )

    val error = intercept[IllegalArgumentException] {
      PreflightValidator.validate(config)
    }

    assert(error.getMessage.contains("staging path for run_id already exists"))
  }

  test("preflight validator should fail when Hive partition registration is enabled without output path") {
    val baseDir = Files.createTempDirectory("preflight-missing-output-")
    val inputFile = Files.createTempFile(baseDir, "input", ".csv")

    val config = AppConfig(
      mode = BatchMode.Daily,
      startDate = "2019-10-01",
      endDate = "2019-10-31",
      inputPath = inputFile.toString,
      stagingBasePath = baseDir.resolve("staging").toString,
      dlqBasePath = baseDir.resolve("dlq").toString,
      runLogBasePath = baseDir.resolve("run-log").toString,
      registerHivePartitions = true,
      outputBasePath = "",
      runId = "run-1"
    )

    val error = intercept[IllegalArgumentException] {
      PreflightValidator.validate(config)
    }

    assert(error.getMessage.contains("output-base-path must be provided"))
  }

  test("preflight validator should pass when execute-wau is enabled with default final output path") {
    val baseDir = Files.createTempDirectory("preflight-execute-wau-")
    val inputFile = Files.createTempFile(baseDir, "input", ".csv")

    val config = AppConfig(
      mode = BatchMode.Daily,
      startDate = "2019-10-01",
      endDate = "2019-10-31",
      inputPath = inputFile.toString,
      stagingBasePath = baseDir.resolve("staging").toString,
      dlqBasePath = baseDir.resolve("dlq").toString,
      runLogBasePath = baseDir.resolve("run-log").toString,
      outputBasePath = baseDir.resolve("final-output").toString,
      executeWau = true,
      runId = "run-1"
    )

    PreflightValidator.validate(config)
  }
}
