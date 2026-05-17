package com.ecommerce.activity.model

import java.sql.Timestamp

final case class SessionState(
    snapshotDateKst: String,
    userId: Long,
    lastSessionId: String,
    lastSessionStartTimeUtc: Timestamp,
    lastEventTimeUtc: Timestamp,
    lastEventTimeKst: Timestamp,
    updatedAt: Timestamp,
    runId: String
)
