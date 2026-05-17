package com.ecommerce.activity.config

import com.ecommerce.activity.model.BatchMode

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
    checkpointBasePath: String = "",
    runId: String = ""
)
