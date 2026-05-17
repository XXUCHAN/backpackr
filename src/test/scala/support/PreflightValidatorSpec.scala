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
}
