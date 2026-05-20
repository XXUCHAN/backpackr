CREATE EXTERNAL TABLE IF NOT EXISTS activity_events (
  event_time_utc timestamp,
  event_time_kst timestamp,
  event_type string,
  product_id bigint,
  category_id bigint,
  category_code string,
  brand string,
  price decimal(18,2),
  user_id bigint,
  raw_user_session string,
  week_start_kst date,
  dedup_key string,
  session_id string,
  session_start_time_utc timestamp,
  session_start_time_kst timestamp,
  session_start_week_kst date,
  ingested_at timestamp,
  run_id string
)
PARTITIONED BY (event_date_kst date)
STORED AS PARQUET
LOCATION '/warehouse/activity_events/';
