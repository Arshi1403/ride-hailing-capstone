"""Publishes mock ride events to Pub/Sub (1000 events over 10 minutes by default)."""
import argparse
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

from google.cloud import pubsub_v1

# NYC taxi zone ids, so events join with dim_zone and line up with the historical data.
ZONES = ["132", "138", "161", "162", "186", "230", "236", "237", "142", "48", "79", "170"]


def make_event(index, day_start):
    pickup = day_start + timedelta(seconds=random.randint(0, 86399 - 3600))
    dropoff = pickup + timedelta(minutes=random.randint(5, 60))
    return {
        "ride_id": f"RIDE-{day_start:%Y%m%d}-{index:05d}",
        "driver_id": f"D{random.randint(1, 100):04d}",
        "rider_id": f"U{random.randint(1, 500):04d}",
        "pickup_zone": random.choice(ZONES),
        "dropoff_zone": random.choice(ZONES),
        "pickup_ts": pickup.isoformat(),
        "dropoff_ts": dropoff.isoformat(),
        "fare": round(random.uniform(8, 100), 2),
        "surge_multiplier": random.choice([1.0, 1.0, 1.0, 1.25, 1.5, 2.0]),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=os.getenv("GOOGLE_CLOUD_PROJECT", "labs-507309"))
    parser.add_argument("--topic", default="rides-events")
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--duration", type=int, default=600, help="seconds")
    parser.add_argument("--days-ago", type=int, default=1, help="UTC day the rides are stamped with")
    args = parser.parse_args()

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    day_start = today - timedelta(days=args.days_ago)

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project, args.topic)
    interval = args.duration / args.events
    start = time.monotonic()
    futures = []

    for i in range(1, args.events + 1):
        payload = json.dumps(make_event(i, day_start)).encode("utf-8")
        futures.append(publisher.publish(topic_path, payload))
        if i % 100 == 0:
            print(f"Published {i}/{args.events}")
        delay = start + i * interval - time.monotonic()
        if delay > 0:
            time.sleep(delay)

    failed = 0
    for f in futures:
        try:
            f.result(timeout=60)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"Publish failed: {exc}")
    print(f"Done. Published {args.events - failed}, failed {failed}.")


if __name__ == "__main__":
    main()
