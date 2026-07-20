#!/usr/bin/env python3
"""
Demand Forecaster
Consumes POS orders from the `live_orders` topic, maintains a rolling
time-bucketed aggregate per product, and periodically forecasts 7-day-ahead
demand per product using Prophet.
"""

import json
import logging
import time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from kafka.serializer import DefaultSerializer, Deserializer, Serializer

# Prophet's fit routine logs Stan sampler chatter at INFO level by default;
# quiet it down before importing prophet so it doesn't drown out our own output.
logging.getLogger("cmdstanpy").setLevel(logging.WARNING)
logging.getLogger("prophet").setLevel(logging.WARNING)
from prophet import Prophet  # noqa: E402

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "live_orders"
FORECASTS_TOPIC_NAME = "demand_forecasts"
CONSUMER_GROUP_ID = "demand-forecaster"

BATCH_TRIGGER_SIZE = 75        # forecast every 75 orders (within the requested 50-100 range)
TIME_BUCKET = "min"            # aggregation granularity - see note in aggregate_batch()
FORECAST_HORIZON_DAYS = 7
MIN_BUCKETS_TO_FORECAST = 2    # Prophet's hard minimum number of points to fit at all


class JSONDeserializer(Deserializer):
    """kafka-python's own JsonSerializer is broken in 3.0.x (deserialize() returns None
    because its super().deserialize() call resolves wrong); this replaces it."""

    def deserialize(self, topic, headers, data):
        if data is None:
            return None
        return json.loads(data)


class JSONSerializer(Serializer):
    """Matches the serializer used by pos_simulator.py - the built-in JsonSerializer
    is broken in kafka-python 3.0.x (see JSONDeserializer above)."""

    def serialize(self, topic, headers, data):
        return json.dumps(data).encode("utf-8")


def create_consumer(broker: str, topic: str, retries: int = 10, retry_delay: float = 3.0) -> KafkaConsumer:
    """Connect to the Kafka/Redpanda broker, retrying while it warms up."""
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=broker,
                value_deserializer=JSONDeserializer(),
                key_deserializer=DefaultSerializer(),  # built-in; confirmed round-trips utf-8 correctly
                auto_offset_reset="earliest",  # replay whatever's already on the topic to seed history
                enable_auto_commit=True,
                group_id=CONSUMER_GROUP_ID,
                bootstrap_timeout_ms=5000,  # fail fast per attempt instead of the 30s default
            )
            print(f"[OK] Connected to broker at {broker}, subscribed to '{topic}'\n")
            return consumer
        except KafkaError:
            print(
                f"[WAIT] Broker not reachable at {broker} "
                f"(attempt {attempt}/{retries}). Is Redpanda running? Retrying in {retry_delay:.0f}s..."
            )
            time.sleep(retry_delay)
    raise ConnectionError(f"Could not reach Kafka broker at {broker} after {retries} attempts.")


def create_producer(broker: str, retries: int = 10, retry_delay: float = 3.0) -> KafkaProducer:
    """Connect to the Kafka/Redpanda broker, retrying while it warms up."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=JSONSerializer(),
                key_serializer=DefaultSerializer(),
                acks="all",
                retries=5,
                bootstrap_timeout_ms=5000,
            )
            print(f"[OK] Forecast producer connected to broker at {broker}\n")
            return producer
        except KafkaError:
            print(
                f"[WAIT] Broker not reachable at {broker} "
                f"(attempt {attempt}/{retries}). Is Redpanda running? Retrying in {retry_delay:.0f}s..."
            )
            time.sleep(retry_delay)
    raise ConnectionError(f"Could not reach Kafka broker at {broker} after {retries} attempts.")


def aggregate_batch(raw_events: list) -> pd.DataFrame:
    """
    Bucket a batch of raw order events into (product_id, time_bucket) -> total quantity.

    TIME_BUCKET is minute-level rather than daily on purpose: at the producer's
    simulated pace (0.2-1.5s between orders), 75 orders arrive within roughly a
    minute of wall-clock time, so daily buckets would collapse an entire batch
    into a single point and Prophet would have nothing to fit a trend against.
    Against real order history spanning multiple days, change TIME_BUCKET to
    "D" to bucket by calendar day instead.
    """
    df = pd.DataFrame(raw_events)
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_convert(None)  # Prophet rejects tz-aware ds

    return (
        df.groupby(["product_id", pd.Grouper(key="timestamp", freq=TIME_BUCKET)])["quantity"]
        .sum()
        .reset_index()
    )


def merge_rolling(rolling_df: pd.DataFrame, new_bucketed: pd.DataFrame) -> pd.DataFrame:
    """Fold newly-bucketed data into the running history, merging any bucket the two share
    (the last bucket of one batch and the first bucket of the next often land on the same
    minute)."""
    combined = pd.concat([rolling_df, new_bucketed], ignore_index=True)
    return combined.groupby(["product_id", "timestamp"])["quantity"].sum().reset_index()


def forecast_demand(rolling_df: pd.DataFrame, producer: KafkaProducer) -> None:
    """Fit one Prophet model per product on the rolling history, print a 7-day forecast,
    and publish it to FORECASTS_TOPIC_NAME for downstream consumers (e.g. inventory_optimizer.py)."""
    print("\n" + "=" * 60)
    print(f" FORECAST - {len(rolling_df)} historical buckets across "
          f"{rolling_df['product_id'].nunique()} product(s)")
    print("=" * 60)

    for product_id in sorted(rolling_df["product_id"].unique()):
        series = rolling_df.loc[rolling_df["product_id"] == product_id, ["timestamp", "quantity"]]

        if len(series) < MIN_BUCKETS_TO_FORECAST:
            print(f"\n[{product_id}] Skipped - only {len(series)} bucket(s) so far "
                  f"(need >= {MIN_BUCKETS_TO_FORECAST}).")
            continue

        prophet_df = series.rename(columns={"timestamp": "ds", "quantity": "y"})

        try:
            model = Prophet(
                growth="flat",             # too little history to trust a fitted trend slope -
                                            # extrapolating a linear trend from minutes of data across
                                            # a 7-day horizon blows up (verified: it produces 5-figure
                                            # forecasts from data points in the 10-25 range). Flat growth
                                            # forecasts around the fitted level instead of a slope.
                daily_seasonality=False,
                weekly_seasonality=False,
                yearly_seasonality=False,
            )
            model.fit(prophet_df)

            future = model.make_future_dataframe(
                periods=FORECAST_HORIZON_DAYS, freq="D", include_history=False
            )
            forecast = model.predict(future)
            forecast[["yhat", "yhat_lower", "yhat_upper"]] = forecast[
                ["yhat", "yhat_lower", "yhat_upper"]
            ].clip(lower=0)

            total = forecast["yhat"].sum()
            confidence_note = " (low confidence - limited history)" if len(series) < 10 else ""
            print(f"\n[{product_id}] 7-day forecasted demand: {total:,.0f} units"
                  f" (fit on {len(series)} buckets){confidence_note}")
            for _, row in forecast.iterrows():
                print(f"    {row['ds'].strftime('%Y-%m-%d')}:  {row['yhat']:7.1f}  "
                      f"(range {row['yhat_lower']:.1f} - {row['yhat_upper']:.1f})")

            payload = {
                "product_id": product_id,
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "daily_forecasted_demand": {
                    row["ds"].strftime("%Y-%m-%d"): round(float(row["yhat"]), 2)
                    for _, row in forecast.iterrows()
                },
            }
            metadata = producer.send(
                FORECASTS_TOPIC_NAME, key=product_id, value=payload
            ).get(timeout=10)
            print(f"    -> published to '{FORECASTS_TOPIC_NAME}' "
                  f"(partition {metadata.partition}, offset {metadata.offset})")

        except Exception as e:
            print(f"\n[{product_id}] Forecast failed: {e}")

    print("\n" + "=" * 60 + "\n")


def main():
    print("=" * 60)
    print(" DEMAND FORECASTER")
    print(f" Topic:            {TOPIC_NAME}")
    print(f" Forecasts topic:  {FORECASTS_TOPIC_NAME}")
    print(f" Broker:           {KAFKA_BROKER}")
    print(f" Forecast trigger: every {BATCH_TRIGGER_SIZE} orders")
    print(f" Time bucket:      {TIME_BUCKET}")
    print("=" * 60 + "\n")

    consumer = create_consumer(KAFKA_BROKER, TOPIC_NAME)
    producer = create_producer(KAFKA_BROKER)

    raw_buffer = []
    rolling_df = pd.DataFrame(columns=["product_id", "timestamp", "quantity"])
    total_consumed = 0

    try:
        for message in consumer:
            event = message.value
            if event is None:
                continue

            total_consumed += 1
            raw_buffer.append(event)
            print(f"[{total_consumed}] consumed {event['product_id']:<10} "
                  f"x{event['quantity']:<3} @ {event['store_location']:<25} "
                  f"(batch {len(raw_buffer)}/{BATCH_TRIGGER_SIZE})")

            if len(raw_buffer) >= BATCH_TRIGGER_SIZE:
                bucketed = aggregate_batch(raw_buffer)
                rolling_df = merge_rolling(rolling_df, bucketed)
                raw_buffer.clear()
                forecast_demand(rolling_df, producer)

    except KeyboardInterrupt:
        print(f"\n\nShutting down. Total orders consumed: {total_consumed}")
    finally:
        consumer.close()
        producer.flush()
        producer.close()
        print("Consumer and producer closed cleanly.")


if __name__ == "__main__":
    main()
