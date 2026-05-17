SELECT
  date_sub(next_day(event_date_kst, 'MON'), 7) AS week_start_kst,
  COUNT(DISTINCT user_id) AS wau_users
FROM activity_events
GROUP BY date_sub(next_day(event_date_kst, 'MON'), 7)
ORDER BY week_start_kst;
