CREATE EXTERNAL TABLE IF NOT EXISTS wau_users_by_week (
  wau_users bigint
)
PARTITIONED BY (week_start_kst date)
STORED AS PARQUET
LOCATION '/warehouse/wau_users_by_week/';
