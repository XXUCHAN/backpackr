CREATE EXTERNAL TABLE IF NOT EXISTS weekly_active_sessions_by_week (
  weekly_active_sessions bigint
)
PARTITIONED BY (week_start_kst date)
STORED AS PARQUET
LOCATION '/warehouse/weekly_active_sessions_by_week/';
