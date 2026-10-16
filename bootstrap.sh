#!/usr/bin/env bash
# Provisions GCP resources for the capstone. Run from the repo root (Cloud Shell works).
#   PROJECT_ID=labs-507309 ALERT_EMAIL=me@example.com ./bootstrap.sh [infra|composer|deploy|all]
# Re-runnable: existing resources are left untouched.
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${REGION:-us-central1}"
BUCKET="${PROJECT_ID}-ride-hailing"
COMPOSER_ENV="${COMPOSER_ENV:-ride-hailing}"
TOPIC="rides-events"
SUBSCRIPTION="rides-events-bq-sub"
BQ_MAX_BYTES=10737418240
PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
PUBSUB_SA="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"

render() { sed -e "s/{{ params.project }}/${PROJECT_ID}/g" -e "s/{{ params.bucket }}/${BUCKET}/g" "$1"; }
bq_run() { bq --project_id="$PROJECT_ID" --location=US query --use_legacy_sql=false --maximum_bytes_billed="$BQ_MAX_BYTES" "$@"; }

infra() {
  gcloud config set project "$PROJECT_ID"
  gcloud services enable pubsub.googleapis.com bigquery.googleapis.com storage.googleapis.com \
    dataproc.googleapis.com composer.googleapis.com secretmanager.googleapis.com billingbudgets.googleapis.com

  # GCS bucket; raw/ objects expire after 14 days.
  gcloud storage buckets describe "gs://$BUCKET" >/dev/null 2>&1 || gcloud storage buckets create "gs://$BUCKET" --location=US
  cat > /tmp/lifecycle.json <<'EOF'
{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 14, "matchesPrefix": ["raw/"]}}]}
EOF
  gcloud storage buckets update "gs://$BUCKET" --lifecycle-file=/tmp/lifecycle.json

  # BigQuery datasets.
  for ds in raw_ds curated_ds mart_ds; do
    bq --project_id="$PROJECT_ID" show "$ds" >/dev/null 2>&1 || bq --project_id="$PROJECT_ID" --location=US mk -d "$ds"
  done

  # Tables (stream landing table, fact, SCD2 dim).
  render sql/ddl.sql | bq_run
  if ! bq --project_id="$PROJECT_ID" show --schema --format=prettyjson "raw_ds.rides_stream" | grep -q '"name": "data"'; then
    echo "raw_ds.rides_stream exists with an old schema (no 'data' column). Drop it and re-run:" >&2
    echo "  bq rm -t ${PROJECT_ID}:raw_ds.rides_stream" >&2
    exit 1
  fi

  # Pub/Sub topic -> BigQuery subscription (replaces Dataflow for live data).
  gcloud pubsub topics describe "$TOPIC" >/dev/null 2>&1 || gcloud pubsub topics create "$TOPIC"
  for role in roles/bigquery.dataEditor roles/bigquery.metadataViewer; do
    gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:${PUBSUB_SA}" --role="$role" --condition=None >/dev/null
  done
  gcloud pubsub subscriptions describe "$SUBSCRIPTION" >/dev/null 2>&1 || \
    gcloud pubsub subscriptions create "$SUBSCRIPTION" --topic="$TOPIC" \
      --bigquery-table="${PROJECT_ID}:raw_ds.rides_stream" --write-metadata

  # Historical subset (NYC Taxi 2018, ~10% sample of Jan 1-14) exported as CSV.
  if ! gcloud storage ls "gs://$BUCKET/historical/trips_000000000000.csv" >/dev/null 2>&1; then
    bq_run "EXPORT DATA OPTIONS (uri='gs://$BUCKET/historical/trips_*.csv', format='CSV', overwrite=true, header=true) AS
      SELECT pickup_datetime, dropoff_datetime, trip_distance, fare_amount, total_amount,
             CAST(pickup_location_id AS STRING) AS pickup_location_id,
             CAST(dropoff_location_id AS STRING) AS dropoff_location_id
      FROM \`bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2018\`
      WHERE pickup_datetime >= '2018-01-01' AND pickup_datetime < '2018-01-15'
        AND MOD(ABS(FARM_FINGERPRINT(CONCAT(CAST(pickup_datetime AS STRING), CAST(dropoff_datetime AS STRING),
            CAST(pickup_location_id AS STRING), CAST(dropoff_location_id AS STRING), CAST(fare_amount AS STRING)))), 10) = 0"
  fi

  # Zone dimension.
  bq_run "EXPORT DATA OPTIONS (uri='gs://$BUCKET/reference/dim_zone/zones_*.csv', format='CSV', overwrite=true, header=true) AS
    SELECT CAST(zone_id AS STRING) AS zone_id, zone_name
    FROM \`bigquery-public-data.new_york_taxi_trips.taxi_zone_geom\`"

  # Daily driver snapshots for the last 8 days; some drivers change vehicle/address 3 days ago (SCD2 demo).
  change_date="$(date -u -d '-3 day' +%F)"
  for i in $(seq 0 8); do
    dt="$(date -u -d "-$i day" +%F)"
    awk -v dt="$dt" -v chg="$change_date" 'BEGIN {
      print "driver_id,driver_name,address,vehicle_type,rating"
      split("Sedan SUV Van", v, " ")
      for (i = 1; i <= 100; i++) {
        vehicle = v[(i % 3) + 1]; addr = sprintf("%d Maple St", i * 7)
        if (dt >= chg && i % 10 == 0) vehicle = "Electric"
        if (dt >= chg && i % 15 == 0) addr = sprintf("%d Oak Ave", i * 11)
        printf "D%04d,Driver %d,%s,%s,%.1f\n", i, i, addr, vehicle, 3 + (i % 20) / 10
      }
    }' | gcloud storage cp - "gs://$BUCKET/raw/driver_snapshot/dt=$dt/drivers.csv"
  done

  gcloud storage cp spark/process_rides.py "gs://$BUCKET/spark/process_rides.py"

  # Dataproc Serverless needs Private Google Access on the subnet and roles on the service account.
  gcloud compute networks subnets update default --region="$REGION" --enable-private-ip-google-access || \
    echo "WARNING: enable Private Google Access on the subnet used by Dataproc Serverless." >&2
  for role in roles/composer.worker roles/dataproc.editor roles/dataproc.worker roles/bigquery.admin \
              roles/storage.admin roles/secretmanager.secretAccessor roles/iam.serviceAccountUser; do
    gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:${SA}" --role="$role" --condition=None >/dev/null
  done

  # Secret read by Airflow through the Secret Manager backend (Variable alert_email).
  if [[ -n "${ALERT_EMAIL:-}" ]]; then
    if gcloud secrets describe airflow-variables-alert_email >/dev/null 2>&1; then
      printf %s "$ALERT_EMAIL" | gcloud secrets versions add airflow-variables-alert_email --data-file=-
    else
      printf %s "$ALERT_EMAIL" | gcloud secrets create airflow-variables-alert_email --data-file=-
    fi
  fi

  # Budget alerts at $50 and $100 (needs a linked billing account).
  billing="$(gcloud billing projects describe "$PROJECT_ID" --format='value(billingAccountName)' | sed 's#billingAccounts/##')"
  if [[ -n "$billing" ]]; then
    for amount in 50 100; do
      gcloud billing budgets list --billing-account="$billing" --format='value(displayName)' | grep -qx "ride-hailing-$amount" || \
        gcloud billing budgets create --billing-account="$billing" --display-name="ride-hailing-$amount" \
          --budget-amount="${amount}USD" --threshold-rule=percent=1.0 || \
        echo "WARNING: could not create the \$$amount budget; create it in Billing > Budgets & alerts." >&2
    done
  fi
}

composer_env() {
  # Smallest environment; delete it when finished: gcloud composer environments delete $COMPOSER_ENV --location $REGION
  gcloud composer environments describe "$COMPOSER_ENV" --location "$REGION" >/dev/null 2>&1 || \
    gcloud composer environments create "$COMPOSER_ENV" --location "$REGION" \
      --image-version composer-3-airflow-2 --environment-size small --service-account "$SA" \
      --airflow-configs='^#^secrets-backend=airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend#secrets-backend_kwargs={"connections_prefix":"airflow-connections","variables_prefix":"airflow-variables"}'
}

deploy() {
  dag_prefix="$(gcloud composer environments describe "$COMPOSER_ENV" --location "$REGION" --format='value(config.dagGcsPrefix)')"
  gcloud storage cp dags/ride_hailing_daily.py "$dag_prefix/"
  gcloud storage rsync --recursive sql "$dag_prefix/sql"
  gcloud storage cp spark/process_rides.py "gs://$BUCKET/spark/process_rides.py"
}

case "${1:-all}" in
  infra) infra ;;
  composer) composer_env ;;
  deploy) deploy ;;
  all) infra; composer_env; deploy ;;
  *) echo "usage: $0 [infra|composer|deploy|all]" >&2; exit 1 ;;
esac
