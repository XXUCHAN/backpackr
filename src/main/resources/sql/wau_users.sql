SELECT
  week_start_kst,
  COUNT(DISTINCT user_id) AS wau_users
FROM activity_events
GROUP BY week_start_kst
ORDER BY week_start_kst;
