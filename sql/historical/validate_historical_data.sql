SELECT
  COUNT(*) AS clean_trips,
  COUNTIF(pickup_date IS NULL) AS missing_pickup_dates,
  COUNTIF(pickup_datetime IS NULL) AS missing_pickup_timestamps,
  COUNTIF(dropoff_datetime IS NULL) AS missing_dropoff_timestamps,
  COUNTIF(dropoff_datetime <= pickup_datetime) AS invalid_timestamp_order,
  COUNTIF(trip_distance <= 0) AS invalid_distances,
  COUNTIF(fare_amount < 0 OR total_amount < 0) AS negative_amounts,
  COUNTIF(
    pickup_date < DATE '2024-01-01'
    OR pickup_date >= DATE '2024-02-01'
  ) AS rows_outside_january
FROM
  `labs-507309.raw_ds.nyc_taxi_2024_01_clean`;
