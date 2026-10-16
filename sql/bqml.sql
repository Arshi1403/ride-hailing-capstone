-- Trained on the 2018 history series only: it is the only contiguous hourly series
-- (live rides cover a single recent day, so mixing both would leave a multi-year gap).
CREATE OR REPLACE MODEL `{{ params.project }}.mart_ds.zone_demand_arima`
OPTIONS (
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'hour_ts',
  time_series_data_col = 'trips',
  time_series_id_col = 'pickup_zone',
  data_frequency = 'HOURLY',
  horizon = 168
) AS
WITH history AS (
  SELECT pickup_zone, hour_ts, trips
  FROM `{{ params.project }}.mart_ds.v_zone_demand_hourly`
  WHERE hour_ts < TIMESTAMP '2019-01-01'
),
top_zones AS (
  SELECT pickup_zone FROM history GROUP BY pickup_zone ORDER BY SUM(trips) DESC LIMIT 10
)
SELECT * FROM history WHERE pickup_zone IN (SELECT pickup_zone FROM top_zones);

CREATE OR REPLACE TABLE `{{ params.project }}.mart_ds.zone_demand_forecast` AS
SELECT
  pickup_zone,
  forecast_timestamp,
  forecast_value,
  prediction_interval_lower_bound,
  prediction_interval_upper_bound
FROM ML.FORECAST(
  MODEL `{{ params.project }}.mart_ds.zone_demand_arima`,
  STRUCT(168 AS horizon, 0.9 AS confidence_level)
);
