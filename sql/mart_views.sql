CREATE OR REPLACE VIEW `{{ params.project }}.mart_ds.v_daily_revenue` AS
SELECT
  pickup_date,
  COUNT(*) AS trips,
  ROUND(SUM(fare), 2) AS revenue,
  COUNT(DISTINCT driver_id) AS active_drivers,
  ROUND(AVG(surge_multiplier), 2) AS avg_surge
FROM `{{ params.project }}.curated_ds.fact_rides`
GROUP BY pickup_date;

CREATE OR REPLACE VIEW `{{ params.project }}.mart_ds.v_zone_demand_hourly` AS
SELECT
  pickup_zone,
  ANY_VALUE(pickup_zone_name) AS pickup_zone_name,
  TIMESTAMP_TRUNC(pickup_ts, HOUR) AS hour_ts,
  COUNT(*) AS trips
FROM `{{ params.project }}.curated_ds.fact_rides`
GROUP BY pickup_zone, hour_ts;

CREATE OR REPLACE VIEW `{{ params.project }}.mart_ds.v_driver_leaderboard` AS
SELECT
  f.driver_id,
  d.driver_name,
  d.vehicle_type,
  COUNT(*) AS trips,
  ROUND(SUM(f.fare), 2) AS revenue,
  ROUND(AVG(f.fare), 2) AS avg_fare,
  RANK() OVER (ORDER BY SUM(f.fare) DESC) AS revenue_rank
FROM `{{ params.project }}.curated_ds.fact_rides` f
LEFT JOIN `{{ params.project }}.curated_ds.dim_driver_scd2` d
  ON d.driver_id = f.driver_id AND d.is_current
WHERE f.driver_id IS NOT NULL
GROUP BY f.driver_id, d.driver_name, d.vehicle_type;

-- Cost report (dataset location is US).
CREATE OR REPLACE VIEW `{{ params.project }}.mart_ds.v_cost_daily_bytes` AS
SELECT
  DATE(creation_time) AS day,
  COUNT(*) AS jobs,
  ROUND(SUM(total_bytes_billed) / POW(1024, 3), 3) AS gb_billed
FROM `{{ params.project }}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE job_type = 'QUERY'
  AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY day;

CREATE OR REPLACE VIEW `{{ params.project }}.mart_ds.v_cost_top_queries` AS
SELECT
  job_id,
  user_email,
  creation_time,
  ROUND(total_bytes_billed / POW(1024, 3), 3) AS gb_billed,
  total_slot_ms,
  SUBSTR(query, 1, 300) AS query_text
FROM `{{ params.project }}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE job_type = 'QUERY'
  AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND total_bytes_billed IS NOT NULL
ORDER BY total_bytes_billed DESC
LIMIT 50;
