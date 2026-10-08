# Ride-Hailing Analytics Platform

A GCP-based data engineering capstone project for real-time and historical
ride-hailing analytics, data quality, driver SCD Type 2, and demand forecasting.

## Architecture

The platform uses:

- Pub/Sub
- Dataflow
- Google Cloud Storage
- BigQuery
- Dataproc Serverless
- Cloud Composer
- BigQuery ML
- Looker Studio

## Data Flow

Streaming:

Python Producer → Pub/Sub → Dataflow → BigQuery Raw

Historical:

CSV → GCS → BigQuery Raw

Processing:

BigQuery Raw → Dataproc Serverless → Curated → BigQuery Mart

Analytics:

BigQuery Mart → BigQuery ML / Looker Studio

## Documentation

- [Architecture](docs/architecture.png)
- [Design Document](docs/design_doc.md)

