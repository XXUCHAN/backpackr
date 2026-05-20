WITH sessions AS (
  SELECT DISTINCT
    session_id,
    session_start_week_kst AS week_start_kst
  FROM activity_events
)
SELECT
  week_start_kst,
  COUNT(*) AS weekly_active_sessions
FROM sessions
GROUP BY week_start_kst
ORDER BY week_start_kst;
