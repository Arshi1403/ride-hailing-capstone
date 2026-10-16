-- Rendered by bootstrap.sh ({{ params.* }} placeholders are substituted).

-- Landing table for the Pub/Sub BigQuery subscription (--write-metadata, no topic schema).
CREATE TABLE IF NOT EXISTS `{{ params.project }}.raw_ds.rides_stream` (
  subscription_name STRING,
  message_id STRING,
  publish_time TIMESTAMP,
  attributes STRING,
  data STRING
)
PARTITION BY DATE(publish_time);

CREATE TABLE IF NOT EXISTS `{{ params.project }}.curated_ds.fact_rides` (
  ride_id STRING,
  driver_id STRING,
  rider_id STRING,
  pickup_zone STRING,
  dropoff_zone STRING,
  pickup_zone_name STRING,
  dropoff_zone_name STRING,
  vehicle_type STRING,
  pickup_ts TIMESTAMP,
  dropoff_ts TIMESTAMP,
  fare FLOAT64,
  trip_distance FLOAT64,
  surge_multiplier FLOAT64,
  trip_minutes FLOAT64,
  revenue_per_km FLOAT64,
  surge_flag BOOL,
  weekday STRING,
  hour_bucket INT64,
  source STRING,
  ingest_ts TIMESTAMP,
  pickup_date DATE
)
PARTITION BY pickup_date
CLUSTER BY pickup_zone, driver_id;

CREATE TABLE IF NOT EXISTS `{{ params.project }}.curated_ds.dim_driver_scd2` (
  driver_id STRING,
  driver_name STRING,
  address STRING,
  vehicle_type STRING,
  rating FLOAT64,
  attr_hash STRING,
  valid_from DATE,
  valid_to DATE,
  is_current BOOL
);
