package config

import model.BatchMode

final case class AppConfig(
    appName: String = "activity-etl-wau",
    mode: BatchMode = BatchMode.Daily,
    startDate: String = "",
    endDate: String = "",
    lookbackDays: Int = 0,
    inputPath: String = "",
    outputBasePath: String = "output/final-output",
    stagingBasePath: String = "output/staging",
    dlqBasePath: String = "output/dlq",
    sessionStateBasePath: String = "output/session-state",
    runLogBasePath: String = "output/run-log",
    wauOutputBasePath: String = "output/wau-results",
    hiveTableName: String = "activity_events",
    checkpointBasePath: String = "",
    runId: String = ""
)
