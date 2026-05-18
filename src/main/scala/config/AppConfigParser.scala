package config

import model.BatchMode
import scopt.OParser

object AppConfigParser {
  def parse(args: Array[String]): AppConfig = {
    val builder = OParser.builder[AppConfig]

    val parser = {
      import builder._

      OParser.sequence(
        programName("activity-etl-wau"),
        head("activity-etl-wau", "0.1.0"),
        opt[String]("app-name")
          .action((value, config) => config.copy(appName = value)),
        opt[String]("mode")
          .required()
          .validate {
            case "daily" | "backfill" => success
            case other                => failure(s"Unsupported mode: $other")
          }
          .action((value, config) => config.copy(mode = BatchMode.from(value))),
        opt[String]("start-date")
          .required()
          .action((value, config) => config.copy(startDate = value)),
        opt[String]("end-date")
          .required()
          .action((value, config) => config.copy(endDate = value)),
        opt[Int]("lookback-days")
          .action((value, config) => config.copy(lookbackDays = value)),
        opt[String]("input-path")
          .required()
          .action((value, config) => config.copy(inputPath = value)),
        opt[String]("output-base-path")
          .action((value, config) => config.copy(outputBasePath = value)),
        opt[String]("staging-base-path")
          .required()
          .action((value, config) => config.copy(stagingBasePath = value)),
        opt[String]("dlq-base-path")
          .required()
          .action((value, config) => config.copy(dlqBasePath = value)),
        opt[String]("session-state-base-path")
          .action((value, config) => config.copy(sessionStateBasePath = value)),
        opt[String]("run-log-base-path")
          .action((value, config) => config.copy(runLogBasePath = value)),
        opt[String]("hive-table-name")
          .action((value, config) => config.copy(hiveTableName = value)),
        opt[Unit]("register-hive-partitions")
          .action((_, config) => config.copy(registerHivePartitions = true)),
        opt[String]("checkpoint-base-path")
          .action((value, config) => config.copy(checkpointBasePath = value)),
        opt[String]("run-id")
          .required()
          .action((value, config) => config.copy(runId = value))
      )
    }

    OParser.parse(parser, args, AppConfig()) match {
      case Some(config) => config
      case None =>
        throw new IllegalArgumentException("Invalid arguments. See usage above.")
    }
  }
}
