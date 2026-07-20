#!/usr/bin/env python3
"""
POS Simulator
Continuously generates synthetic Point of Sale events and publishes them to
the `live_orders` Kafka/Redpanda topic, simulating live retail traffic.
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.serializer import Serializer

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "live_orders"
PRODUCTS = ["Widget_A", "Widget_B", "Widget_C"]
MIN_DELAY_SECONDS = 0.2
MAX_DELAY_SECONDS = 1.5

fake = Faker()


class JSONSerializer(Serializer):
    def serialize(self, topic, headers, data):
        return json.dumps(data).encode("utf-8")


class StringSerializer(Serializer):
    def serialize(self, topic, headers, data):
        return data.encode("utf-8")

# Fixed pool of store locations, generated once at startup. Using a finite,
# repeating set (instead of a fresh random city per event) is what makes the
# stream realistic for a supply chain use case — you need the same handful
# of stores to reappear so downstream consumers can aggregate by location.
STORE_LOCATIONS = [f"{fake.city()}, {fake.state_abbr()}" for _ in range(6)]


def create_producer(broker: str, retries: int = 10, retry_delay: float = 3.0) -> KafkaProducer:
    """Connect to the Kafka/Redpanda broker, retrying while it warms up."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=JSONSerializer(),
                key_serializer=StringSerializer(),
                acks="all",
                retries=5,
                bootstrap_timeout_ms=5000,  # fail fast per attempt instead of the 30s default
            )
            print(f"[OK] Connected to broker at {broker}\n")
            return producer
        except KafkaError:
            print(
                f"[WAIT] Broker not reachable at {broker} "
                f"(attempt {attempt}/{retries}). Is `docker compose up` running? "
                f"Retrying in {retry_delay:.0f}s..."
            )
            time.sleep(retry_delay)
    raise ConnectionError(f"Could not reach Kafka broker at {broker} after {retries} attempts.")


def generate_pos_event() -> dict:
    """Build a single synthetic Point of Sale order."""
    return {
        "order_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "product_id": random.choice(PRODUCTS),
        "quantity": random.randint(1, 20),
        "store_location": random.choice(STORE_LOCATIONS),
    }


def main():
    print("=" * 60)
    print(" POS SIMULATOR - live retail order stream")
    print(f" Topic:  {TOPIC_NAME}")
    print(f" Broker: {KAFKA_BROKER}")
    print(f" Stores: {', '.join(STORE_LOCATIONS)}")
    print("=" * 60 + "\n")

    producer = create_producer(KAFKA_BROKER)
    order_count = 0

    try:
        while True:
            event = generate_pos_event()
            order_count += 1

            print(f"[{order_count}] {event['product_id']:<10} x{event['quantity']:<3} "
                  f"@ {event['store_location']:<25} order_id={event['order_id']}")

            try:
                metadata = producer.send(
                    TOPIC_NAME, key=event["order_id"], value=event
                ).get(timeout=10)
                print(f"    -> delivered to partition {metadata.partition}, offset {metadata.offset}")
            except KafkaError as e:
                print(f"    -> [ERROR] failed to publish order {event['order_id']}: {e}")

            delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
            time.sleep(delay)

    except KeyboardInterrupt:
        print(f"\n\nStopping simulator. Total orders published: {order_count}")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed cleanly.")


if __name__ == "__main__":
    main()
