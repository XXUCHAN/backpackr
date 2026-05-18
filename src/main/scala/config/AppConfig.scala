package config

import model.BatchMode

final case class AppConfig(
    appName: String = "activity-etl-wau",
    mode: BatchMode = BatchMode.Daily,
    startDate: String = "",
    endDate: String = "",
    lookbackDays: Int = 0,
    inputPath: String = "",
    outputBasePath: String = ".tmp/final-output",
    stagingBasePath: String = ".tmp/staging",
    dlqBasePath: String = ".tmp/dlq",
    sessionStateBasePath: String = ".tmp/session-state",
    runLogBasePath: String = ".tmp/batch-run-log",
    wauOutputBasePath: String = ".tmp/wau-results",
    hiveTableName: String = "activity_events",
    registerHivePartitions: Boolean = false,
    executeWau: Boolean = false,
    checkpointBasePath: String = "",
    runId: String = ""
)
