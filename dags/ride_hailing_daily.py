"""ride_hailing_daily: history load -> Spark curated -> BigQuery MERGEs -> marts -> BQML -> DQ gate -> notify."""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT = os.environ.get("GCP_PROJECT", "labs-507309")
REGION = os.environ.get("GCP_REGION", "us-central1")
BUCKET = f"{PROJECT}-ride-hailing"
BQ_LOCATION = "US"
BQ_MAX_BYTES = 10 * 1024**3  # 10 GB cap per query

# Deployed next to the DAG (dags/sql); falls back to the repo layout for local tests.
_here = os.path.dirname(os.path.abspath(__file__))
SQL_DIR = os.path.join(_here, "sql") if os.path.isdir(os.path.join(_here, "sql")) else os.path.join(_here, "..", "sql")

# Data quality thresholds.
DQ_MIN_ROWS_FOR_DS = 1
DQ_MAX_NULL_RATIO = 0.01

DQ_SQL = f"""
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(pickup_date = @ds) AS ds_rows,
  SAFE_DIVIDE(COUNTIF(pickup_ts IS NULL OR dropoff_ts IS NULL OR fare IS NULL OR pickup_zone IS NULL),
              COUNT(*)) AS null_ratio,
  COUNTIF(fare < 0) AS negative_fares,
  COUNT(*) - COUNT(DISTINCT ride_id) AS duplicate_rides
FROM `{PROJECT}.curated_ds.fact_rides`
"""

HISTORY_SCHEMA = [
    {"name": "pickup_datetime", "type": "TIMESTAMP"},
    {"name": "dropoff_datetime", "type": "TIMESTAMP"},
    {"name": "trip_distance", "type": "FLOAT"},
    {"name": "fare_amount", "type": "FLOAT"},
    {"name": "total_amount", "type": "FLOAT"},
    {"name": "pickup_location_id", "type": "STRING"},
    {"name": "dropoff_location_id", "type": "STRING"},
]

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "sla": timedelta(hours=4),
    "on_failure_callback": lambda ctx: notify_failure(ctx),
}


def notify_failure(context):
    """Emails a failed task; alert_email comes from Secret Manager via the Airflow Variables backend."""
    from airflow.models import Variable
    from airflow.utils.email import send_email

    ti = context["task_instance"]
    send_email(
        Variable.get("alert_email"),
        f"[ride_hailing_daily] {ti.task_id} failed for {context['ds']}",
        f"Log: {ti.log_url}",
    )


def bq_job(sql_file):
    return {
        "query": {
            "query": f"{{% include '{sql_file}' %}}",
            "useLegacySql": False,
            "maximumBytesBilled": str(BQ_MAX_BYTES),
        }
    }


def run_dq_checks(ds, **_):
    """Returns metrics and failures; the value is pushed to XCom."""
    from google.cloud import bigquery

    client = BigQueryHook(use_legacy_sql=False).get_client(project_id=PROJECT, location=BQ_LOCATION)
    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=BQ_MAX_BYTES,
        query_parameters=[bigquery.ScalarQueryParameter("ds", "DATE", ds)],
    )
    row = dict(next(iter(client.query(DQ_SQL, job_config=job_config).result())))
    return {"metrics": {k: float(v or 0) for k, v in row.items()}, "failures": evaluate_dq(row)}


def evaluate_dq(m):
    failures = []
    if (m["ds_rows"] or 0) < DQ_MIN_ROWS_FOR_DS:
        failures.append(f"ds_rows={m['ds_rows']} < {DQ_MIN_ROWS_FOR_DS}")
    if (m["null_ratio"] or 0) > DQ_MAX_NULL_RATIO:
        failures.append(f"null_ratio={m['null_ratio']:.4f} > {DQ_MAX_NULL_RATIO}")
    if (m["negative_fares"] or 0) > 0:
        failures.append(f"negative_fares={m['negative_fares']}")
    if (m["duplicate_rides"] or 0) > 0:
        failures.append(f"duplicate_rides={m['duplicate_rides']}")
    return failures


def choose_retrain(ds, **_):
    # Weekly: the Monday run (ds is the Sunday data interval).
    return "retrain_bqml" if datetime.strptime(ds, "%Y-%m-%d").weekday() == 6 else "skip_retrain"


def choose_dq_path(ti, **_):
    result = ti.xcom_pull(task_ids="dq_checks")
    return "alert" if result["failures"] else "notify_success"


with DAG(
    dag_id="ride_hailing_daily",
    description="Daily ride-hailing analytics pipeline",
    schedule="0 2 * * *",  # finishes within the 4h SLA, before 06:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,  # backfill on demand: airflow dags backfill -s <d-7> -e <d-1> ride_hailing_daily
    max_active_runs=1,  # runs in date order, which SCD2 needs
    dagrun_timeout=timedelta(hours=4),
    default_args=default_args,
    params={"project": PROJECT, "bucket": BUCKET},
    template_searchpath=[SQL_DIR],
    tags=["capstone"],
) as dag:
    sense_historical_dump = GCSObjectExistenceSensor(
        task_id="sense_historical_dump",
        bucket=BUCKET,
        object="historical/trips_000000000000.csv",
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )

    load_history_to_raw = GCSToBigQueryOperator(
        task_id="load_history_to_raw",
        bucket=BUCKET,
        source_objects=["historical/trips_*.csv"],
        destination_project_dataset_table=f"{PROJECT}.raw_ds.rides_history",
        schema_fields=HISTORY_SCHEMA,
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        location=BQ_LOCATION,
    )

    run_curated_spark = DataprocCreateBatchOperator(
        task_id="run_curated_spark",
        project_id=PROJECT,
        region=REGION,
        batch_id="rides-{{ ds_nodash }}-{{ dag_run.start_date.strftime('%H%M%S') }}-{{ ti.try_number }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{BUCKET}/spark/process_rides.py",
                "args": ["--project", PROJECT, "--bucket", BUCKET, "--dt", "{{ ds }}"],
            },
            "runtime_config": {
                "version": "2.2",
                "properties": {"spark.dynamicAllocation.maxExecutors": "4"},
            },
            "environment_config": {"execution_config": {"subnetwork_uri": "default"}},
        },
    )

    merge_fact = BigQueryInsertJobOperator(
        task_id="merge_fact", configuration=bq_job("merge_fact.sql"), location=BQ_LOCATION
    )
    merge_dim_scd2 = BigQueryInsertJobOperator(
        task_id="merge_dim_scd2", configuration=bq_job("merge_dim_scd2.sql"), location=BQ_LOCATION
    )
    refresh_mart_views = BigQueryInsertJobOperator(
        task_id="refresh_mart_views", configuration=bq_job("mart_views.sql"), location=BQ_LOCATION
    )

    branch_weekly = BranchPythonOperator(task_id="branch_weekly_retrain", python_callable=choose_retrain)
    retrain_bqml = BigQueryInsertJobOperator(
        task_id="retrain_bqml", configuration=bq_job("bqml.sql"), location=BQ_LOCATION
    )
    skip_retrain = EmptyOperator(task_id="skip_retrain")

    dq_checks = PythonOperator(
        task_id="dq_checks",
        python_callable=run_dq_checks,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    branch_on_dq = BranchPythonOperator(task_id="branch_on_dq", python_callable=choose_dq_path)

    alert = EmailOperator(
        task_id="alert",
        to="{{ var.value.alert_email }}",
        subject="[ride_hailing_daily] DQ FAILED for {{ ds }}",
        html_content="Failures: {{ ti.xcom_pull(task_ids='dq_checks')['failures'] }}",
    )
    notify_success = EmailOperator(
        task_id="notify_success",
        to="{{ var.value.alert_email }}",
        subject="[ride_hailing_daily] OK for {{ ds }}",
        html_content="Metrics: {{ ti.xcom_pull(task_ids='dq_checks')['metrics'] }}",
    )

    (
        sense_historical_dump
        >> load_history_to_raw
        >> run_curated_spark
        >> merge_fact
        >> merge_dim_scd2
        >> refresh_mart_views
        >> branch_weekly
        >> [retrain_bqml, skip_retrain]
        >> dq_checks
        >> branch_on_dq
        >> [alert, notify_success]
    )
