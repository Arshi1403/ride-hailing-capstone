# Ride-Hailing Analytics Platform
## Architecture and Design Document

## 1. Project Overview

The Ride-Hailing Analytics Platform is a cloud-based data engineering pipeline for processing ride-hailing data.

The platform supports:

- Real-time ride event ingestion
- Historical ride data processing
- Data cleaning and transformation
- Driver history tracking using SCD Type 2
- Business analytics
- Demand forecasting
- Data quality checks
- Monitoring and cost control

The main GCP services used are:

- Pub/Sub
- Google Cloud Storage
- BigQuery
- Dataproc Serverless
- Cloud Composer
- BigQuery ML
- Looker Studio

---

## 2. High-Level Architecture

The overall data flow is:

Python Producer
        |
        v
Pub/Sub
        |
        v   (BigQuery subscription)
BigQuery Raw

Historical CSV
        |
        v
Google Cloud Storage
        |
        v
BigQuery Raw

Raw Data
        |
        v
Dataproc Serverless / PySpark
        |
        v
Curated Parquet + BigQuery Curated
        |
        v
BigQuery Mart
        |
        +--------------------+
        |                    |
        v                    v
BigQuery ML            Looker Studio
Forecasting             Dashboard

Cloud Composer orchestrates the complete pipeline.

---

## 3. Data Sources

### Streaming Data

A Python producer generates mock ride events.

The streaming events contain:

- ride_id
- driver_id
- rider_id
- pickup_zone
- dropoff_zone
- pickup_ts
- dropoff_ts
- fare
- surge_multiplier

The producer publishes the events to the Pub/Sub topic:

`rides-events`

The project requires 1000 mock ride events over approximately 10 minutes.

### Historical Data

Historical NYC Taxi CSV data is used as batch input.

The historical files are stored in Google Cloud Storage before being loaded into BigQuery.

---

## 4. Ingestion Layer

### Pub/Sub

Pub/Sub receives streaming ride events from the Python producer.

Topic:

`rides-events`

### Pub/Sub BigQuery subscription

A BigQuery subscription writes messages from Pub/Sub directly into BigQuery (Dataflow is not used because its job failed on every run). The JSON payload lands in the `data` column and `publish_time` is used as `ingest_ts`.

Target table:

`raw_ds.rides_stream`

### Google Cloud Storage

GCS stores the historical CSV data.

The historical data is later loaded into:

`raw_ds.rides_history`

---

## 5. Raw Layer

The Raw layer stores incoming data with minimal transformation.

Main raw tables:

### `raw_ds.rides_stream`

Contains streaming ride events.

### `raw_ds.rides_history`

Contains historical ride data loaded from CSV files.

The Raw layer provides the source data for downstream processing.

---

## 6. Processing Layer

Dataproc Serverless runs PySpark jobs to transform the raw data.

The Spark processing performs:

### Data Cleaning

- Remove records with invalid or missing pickup/dropoff timestamps.
- Remove negative fares.
- Remove trips longer than 4 hours.

### Deduplication

Duplicate ride records are removed using the latest `ingest_ts` for each `ride_id`.

### Enrichment

Ride data is enriched using:

- Driver information
- Zone information

### Derived Fields

The pipeline derives:

- trip_minutes
- revenue_per_km
- surge_flag
- weekday
- hour_bucket

---

## 7. Curated Layer

The cleaned and transformed data is stored as curated Parquet files in GCS.

The curated data is also loaded into BigQuery.

### Main Fact Table

`curated_ds.fact_rides`

This table stores ride-level business data.

Partitioning:

`pickup_date`

Clustering:

- pickup_zone
- driver_id

### Driver Dimension

`curated_ds.dim_driver_scd2`

This table maintains driver history using Slowly Changing Dimension Type 2.

SCD Type 2 allows the system to preserve historical versions of driver information.

---

## 8. Mart Layer

The Mart layer contains business-ready views.

### Daily Revenue

`mart_ds.v_daily_revenue`

Used for daily revenue analysis.

### Zone Demand

`mart_ds.v_zone_demand_hourly`

Used to analyze hourly ride demand by pickup zone.

### Driver Leaderboard

`mart_ds.v_driver_leaderboard`

Used to compare driver performance.

---

## 9. Forecasting

BigQuery ML is used for demand forecasting.

The model uses:

`mart_ds.v_zone_demand_hourly`

The forecasting requirement is to generate demand forecasts for the next:

**168 hours / 7 days**

The forecast focuses on the top 10 zones.

The forecasting model uses:

`ARIMA_PLUS`

---

## 10. Dashboard

Looker Studio provides the analytics dashboard.

The dashboard includes:

- Yesterday's trips
- Yesterday's revenue
- Average surge
- Active drivers
- 30-day revenue trend
- Hourly demand heatmap
- Demand forecast

---

## 11. Cloud Composer Orchestration

Cloud Composer orchestrates the complete daily pipeline.

The main DAG is:

`ride_hailing_daily`

The major tasks are:

1. sense_historical_dump
2. load_history_to_raw
3. run_curated_spark
4. merge_fact
5. merge_dim_scd2
6. refresh_mart_views
7. retrain_bqml
8. dq_checks
9. branch_on_dq
10. notify_success

Composer manages dependencies, retries, scheduling and pipeline execution.

---

## 12. Data Quality

Data quality checks are performed before the pipeline is considered successful.

The checks include:

- Row count checks
- Null checks
- Threshold validation

The result of the checks is passed using XCom.

If the quality checks fail, the DAG branches to an alert path.

If the checks pass, the pipeline continues to successful completion.

---

## 13. Reliability

The pipeline uses:

- 2 retries
- Exponential backoff
- 4-hour SLA
- On-demand backfill for the previous 7 days

These settings improve pipeline reliability.

---

## 14. Cost Controls

The project uses cost-control measures including:

- Smallest practical Composer environment
- Dataproc Serverless
- BigQuery maximum bytes billed of 10 GB
- GCS lifecycle policy for raw data older than 14 days
- Budget alerts at $50 and $100

---

## 15. Overall Data Flow

The complete flow is:

Producer
→ Pub/Sub
→ BigQuery Raw
→ Dataproc Serverless
→ Curated Parquet / BigQuery
→ BigQuery Mart
→ BigQuery ML
→ Looker Studio

Historical data follows:

Historical CSV
→ GCS
→ BigQuery Raw
→ Dataproc Serverless
→ Curated
→ Mart

Cloud Composer controls the overall workflow.
