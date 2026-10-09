import json
import random
import time
from datetime import datetime, timedelta, timezone

from google.cloud import pubsub_v1


PROJECT_ID = "labs-507309"
TOPIC_ID = "rides-events"

NUM_EVENTS = 1000
DURATION_SECONDS = 600

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

zones = [
    "Manhattan",
    "Brooklyn",
    "Queens",
    "Bronx",
    "Staten Island",
]


def create_ride_event(index):
    pickup_time = datetime.now(timezone.utc)

    duration_minutes = random.randint(5, 45)

    dropoff_time = pickup_time + timedelta(
        minutes=duration_minutes
    )

    return {
        "ride_id": f"RIDE-{index:05d}",
        "driver_id": f"DRV-{random.randint(1, 100):04d}",
        "rider_id": f"RDR-{random.randint(1, 500):04d}",
        "pickup_zone": random.choice(zones),
        "dropoff_zone": random.choice(zones),
        "pickup_ts": pickup_time.isoformat(),
        "dropoff_ts": dropoff_time.isoformat(),
        "fare": round(random.uniform(8, 100), 2),
        "surge_multiplier": round(
            random.choice([1.0, 1.0, 1.0, 1.25, 1.5, 2.0]),
            2,
        ),
    }


def publish_event(event):
    message = json.dumps(event).encode("utf-8")

    future = publisher.publish(
        topic_path,
        message,
        ride_id=event["ride_id"],
    )

    future.result()


def main():
    interval = DURATION_SECONDS / NUM_EVENTS

    print(f"Publishing {NUM_EVENTS} ride events...")
    print(f"Target duration: {DURATION_SECONDS} seconds")

    for i in range(1, NUM_EVENTS + 1):
        event = create_ride_event(i)

        publish_event(event)

        print(
            f"Published {i}/{NUM_EVENTS}: "
            f"{event['ride_id']}"
        )

        time.sleep(interval)

    print("Finished publishing events.")


if __name__ == "__main__":
    main()
