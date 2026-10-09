CREATE OR REPLACE TABLE
  `labs-507309.raw_ds.nyc_taxi_2024_01_clean`
PARTITION BY pickup_date
CLUSTER BY pickup_location_id, dropoff_location_id
AS
SELECT
  DATE(tpep_pickup_datetime) AS pickup_date,
  tpep_pickup_datetime AS pickup_datetime,
  tpep_dropoff_datetime AS dropoff_datetime,
  passenger_count,
  trip_distance,
  fare_amount,
  tip_amount,
  total_amount,
  PULocationID AS pickup_location_id,
  DOLocationID AS dropoff_location_id,
  payment_type,
  TIMESTAMP_DIFF(
    tpep_dropoff_datetime,
    tpep_pickup_datetime,
    MINUTE
  ) AS trip_duration_minutes
FROM
  `labs-507309.raw_ds.nyc_taxi_2024_01`
WHERE
  tpep_pickup_datetime >= TIMESTAMP('2024-01-01 00:00:00+00')
  AND tpep_pickup_datetime < TIMESTAMP('2024-02-01 00:00:00+00')
  AND tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND trip_distance > 0
  AND fare_amount >= 0
  AND total_amount >= 0;
