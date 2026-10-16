-- SCD Type 2: a changed driver gets its current row closed and a new current row inserted.
-- Idempotent for a given {{ ds }}; run dates in chronological order when backfilling.
CREATE OR REPLACE EXTERNAL TABLE `{{ params.project }}.curated_ds.ext_driver_snapshot`
WITH PARTITION COLUMNS
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://{{ params.bucket }}/curated/dim_driver_snapshot/*'],
  hive_partition_uri_prefix = 'gs://{{ params.bucket }}/curated/dim_driver_snapshot'
);

MERGE `{{ params.project }}.curated_ds.dim_driver_scd2` T
USING (
  SELECT driver_id AS merge_key, driver_id, driver_name, address, vehicle_type, rating, attr_hash, dt
  FROM `{{ params.project }}.curated_ds.ext_driver_snapshot`
  WHERE dt = DATE '{{ ds }}'
  UNION ALL
  -- Second copy of changed drivers with a NULL key so it falls into WHEN NOT MATCHED.
  SELECT CAST(NULL AS STRING), s.driver_id, s.driver_name, s.address, s.vehicle_type, s.rating, s.attr_hash, s.dt
  FROM `{{ params.project }}.curated_ds.ext_driver_snapshot` s
  JOIN `{{ params.project }}.curated_ds.dim_driver_scd2` c
    ON c.driver_id = s.driver_id AND c.is_current AND c.attr_hash != s.attr_hash
  WHERE s.dt = DATE '{{ ds }}'
) S
ON T.driver_id = S.merge_key AND T.is_current
WHEN MATCHED AND T.attr_hash != S.attr_hash THEN
  UPDATE SET is_current = FALSE, valid_to = S.dt
WHEN NOT MATCHED THEN
  INSERT (driver_id, driver_name, address, vehicle_type, rating, attr_hash, valid_from, valid_to, is_current)
  VALUES (S.driver_id, S.driver_name, S.address, S.vehicle_type, S.rating, S.attr_hash, S.dt, DATE '9999-12-31', TRUE);
