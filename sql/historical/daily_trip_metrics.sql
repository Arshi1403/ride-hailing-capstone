SELECT
  pickup_date,
  COUNT(*) AS total_trips,
  ROUND(SUM(total_amount), 2) AS total_recorded_amount,
  ROUND(AVG(total_amount), 2) AS average_trip_amount,
  ROUND(AVG(trip_distance), 2) AS average_trip_distance,
  ROUND(AVG(trip_duration_minutes), 2) AS average_duration_minutes
FROM
  `labs-507309.raw_ds.nyc_taxi_2024_01_clean`
WHERE
  pickup_date >= DATE '2024-01-01'
  AND pickup_date < DATE '2024-02-01'
GROUP BY
  pickup_date
ORDER BY
  pickup_date;
