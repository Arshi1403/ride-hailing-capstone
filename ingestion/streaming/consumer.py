import json
import logging
import os
from datetime import datetime, timezone

from google.cloud import bigquery, pubsub_v1

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "labs-507309")
SUBSCRIPTION_ID = os.getenv(
    "PUBSUB_SUBSCRIPTION",
    "rides-events-python-consumer-sub",
)
TABLE_ID = os.getenv(
    "BIGQUERY_TABLE",
    "labs-507309.raw_ds.rides_stream",
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

subscriber = pubsub_v1.SubscriberClient()
bq = bigquery.Client(project=PROJECT_ID)
subscription_path = subscriber.subscription_path(
    PROJECT_ID, SUBSCRIPTION_ID
)

def process_message(message):
    try:
        event = json.loads(message.data.decode("utf-8"))

        required_fields = [
            "ride_id", "driver_id", "rider_id",
            "pickup_zone", "dropoff_zone",
            "pickup_ts", "dropoff_ts",
            "fare", "surge_multiplier",
        ]
        missing = [field for field in required_fields if field not in event]
        if missing:
            raise ValueError(f"Missing required fields: {missing}")

        # Timestamp when this consumer processes the message.
        event["ingest_ts"] = datetime.now(timezone.utc).isoformat()

        # Insert one row. Use Pub/Sub message ID for retry deduplication.
        errors = bq.insert_rows_json(
            TABLE_ID,
            [event],
            row_ids=[message.message_id],
        )

        if errors:
            logger.error("BigQuery insert failed: %s", errors)
            message.nack()
            return

        logger.info(
            "Inserted ride_id=%s, ingest_ts=%s",
            event["ride_id"],
            event["ingest_ts"],
        )
        message.ack()

    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
        logger.error("Invalid message %s: %s", message.message_id, exc)
        # Don't retry malformed messages indefinitely.
        message.ack()
    except Exception:
        logger.exception("Processing failed for message %s", message.message_id)
        message.nack()

def main():
    logger.info("Listening on %s", subscription_path)
    streaming_pull = subscriber.subscribe(
        subscription_path,
        callback=process_message,
    )
    try:
        streaming_pull.result()
    except KeyboardInterrupt:
        streaming_pull.cancel()
        streaming_pull.result()
    finally:
        subscriber.close()

if __name__ == "__main__":
    main()
