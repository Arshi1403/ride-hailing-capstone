-- Idempotent: re-running for the same data updates rows in place.
CREATE OR REPLACE EXTERNAL TABLE `{{ params.project }}.curated_ds.ext_rides`
WITH PARTITION COLUMNS
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://{{ params.bucket }}/curated/rides/*'],
  hive_partition_uri_prefix = 'gs://{{ params.bucket }}/curated/rides'
);

MERGE `{{ params.project }}.curated_ds.fact_rides` T
USING (
  SELECT
    ride_id, driver_id, rider_id, pickup_zone, dropoff_zone, pickup_zone_name,
    dropoff_zone_name, vehicle_type, pickup_ts, dropoff_ts, fare, trip_distance,
    surge_multiplier, trip_minutes, revenue_per_km, surge_flag, weekday,
    hour_bucket, source, ingest_ts, dt AS pickup_date
  FROM `{{ params.project }}.curated_ds.ext_rides`
) S
ON T.ride_id = S.ride_id AND T.pickup_date = S.pickup_date
WHEN MATCHED THEN UPDATE SET
  driver_id = S.driver_id, rider_id = S.rider_id, pickup_zone = S.pickup_zone,
  dropoff_zone = S.dropoff_zone, pickup_zone_name = S.pickup_zone_name,
  dropoff_zone_name = S.dropoff_zone_name, vehicle_type = S.vehicle_type,
  pickup_ts = S.pickup_ts, dropoff_ts = S.dropoff_ts, fare = S.fare,
  trip_distance = S.trip_distance, surge_multiplier = S.surge_multiplier,
  trip_minutes = S.trip_minutes, revenue_per_km = S.revenue_per_km,
  surge_flag = S.surge_flag, weekday = S.weekday, hour_bucket = S.hour_bucket,
  source = S.source, ingest_ts = S.ingest_ts
WHEN NOT MATCHED THEN INSERT ROW;
