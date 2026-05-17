WITH sessions AS (
  SELECT DISTINCT
    session_id,
    to_date(session_start_time_kst) AS session_start_date_kst
  FROM activity_events
)
SELECT
  date_sub(next_day(session_start_date_kst, 'MON'), 7) AS week_start_kst,
  COUNT(*) AS weekly_active_sessions
FROM sessions
GROUP BY date_sub(next_day(session_start_date_kst, 'MON'), 7)
ORDER BY week_start_kst;
