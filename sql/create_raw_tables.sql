CREATE TABLE IF NOT EXISTS `labs-507309.raw_ds.rides_stream`
(
  ride_id STRING,
  driver_id STRING,
  rider_id STRING,
  pickup_zone STRING,
  dropoff_zone STRING,
  pickup_ts TIMESTAMP,
  dropoff_ts TIMESTAMP,
  fare FLOAT64,
  surge_multiplier FLOAT64
);
