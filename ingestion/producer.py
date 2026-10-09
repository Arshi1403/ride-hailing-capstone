
import json
import random
import time
from datetime import datetime, timedelta, timezone

from google.cloud import pubsub_v1

PROJECT_ID = "labs-507309"
TOPIC_ID = "rides-events"

TOTAL_EVENTS = 1000
DURATION_SECONDS = 600  # 10 minutes

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

zones = [
    "Manhattan", "Brooklyn", "Queens",
    "Bronx", "JFK", "LaGuardia"
]

def generate_ride_event(index):
    pickup = datetime.now(timezone.utc) - timedelta(
        seconds=random.randint(60, 1800)
    )
    duration = random.randint(300, 2400)
    dropoff = pickup + timedelta(seconds=duration)

    return {
        "ride_id": f"RIDE-{index:05d}",
        "driver_id": f"D{random.randint(1, 100):04d}",
        "rider_id": f"U{random.randint(1, 500):04d}",
        "pickup_zone": random.choice(zones),
        "dropoff_zone": random.choice(zones),
        "pickup_ts": pickup.isoformat(),
        "dropoff_ts": dropoff.isoformat(),
        "fare": round(random.uniform(8, 100), 2),
        "surge_multiplier": round(random.choice([
            1.0, 1.0, 1.0, 1.2, 1.5, 2.0
        ]), 2)
    }

def main():
    interval = DURATION_SECONDS / TOTAL_EVENTS
    start = time.monotonic()
    published = 0

    print(f"Publishing {TOTAL_EVENTS} ride events...")

    for i in range(1, TOTAL_EVENTS + 1):
        event = generate_ride_event(i)
        payload = json.dumps(event).encode("utf-8")

        try:
            future = publisher.publish(topic_path, payload)
            future.result(timeout=30)
            published += 1

            if published % 100 == 0:
                print(f"Published {published}/{TOTAL_EVENTS}")

        except Exception as exc:
            print(f"Failed to publish ride {i}: {exc}")

        target_time = start + i * interval
        remaining = target_time - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)

    publisher.transport.close()
    print(f"Finished. Successfully published: {published}")

if __name__ == "__main__":
    main()
