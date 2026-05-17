package config

import model.BatchMode

final case class AppConfig(
    appName: String = "activity-etl-wau",
    mode: BatchMode = BatchMode.Daily,
    startDate: String = "",
    endDate: String = "",
    lookbackDays: Int = 0,
    inputPath: String = "",
    outputBasePath: String = "",
    stagingBasePath: String = "",
    dlqBasePath: String = "",
    sessionStateBasePath: String = ".tmp/session-state",
    runLogBasePath: String = ".tmp/batch-run-log",
    checkpointBasePath: String = "",
    runId: String = ""
)
