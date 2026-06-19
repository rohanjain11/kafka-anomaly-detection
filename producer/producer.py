"""Kafka producer that simulates IoT sensor readings with injected anomalies."""

import json
import os
import random
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC = "sensor-readings"
NUM_SENSORS = 10
INTERVAL_SECONDS = 2
MAX_RETRIES = 5
RETRY_DELAY_SECONDS = 3


def generate_reading(sensor_id: int) -> dict:
    """Generate one realistic sensor reading dict with optional injected anomaly.

    Args:
        sensor_id: Identifier for the sensor (1-10).

    Returns:
        Dict with sensor_id, timestamp, temperature, vibration, and
        ground_truth_anomaly flag.
    """
    timestamp = time.time()
    is_anomaly = random.random() < 0.05

    if is_anomaly:
        if random.random() < 0.5:
            temperature = random.uniform(95, 130)
            vibration = random.uniform(0.1, 0.5)
        else:
            temperature = random.uniform(60, 80)
            vibration = random.uniform(2.0, 5.0)
        ground_truth_anomaly = True
    else:
        temperature = random.uniform(60, 80)
        vibration = random.uniform(0.1, 0.5)
        ground_truth_anomaly = False

    return {
        "sensor_id": sensor_id,
        "timestamp": timestamp,
        "temperature": round(temperature, 2),
        "vibration": round(vibration, 2),
        "ground_truth_anomaly": ground_truth_anomaly,
    }


def create_producer() -> KafkaProducer:
    """Create and return a configured Kafka producer with retry logic.

    Returns:
        Connected KafkaProducer instance.

    Raises:
        NoBrokersAvailable: If broker is unreachable after all retries.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Connecting to Kafka broker at {KAFKA_BROKER} (attempt {attempt}/{MAX_RETRIES})...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"Connected to Kafka broker at {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            if attempt == MAX_RETRIES:
                print(f"Failed to connect to Kafka after {MAX_RETRIES} attempts")
                raise
            print(f"Kafka not ready, retrying in {RETRY_DELAY_SECONDS}s...")
            time.sleep(RETRY_DELAY_SECONDS)

    raise NoBrokersAvailable()


def main() -> None:
    """Run the producer loop, sending one reading per sensor every 2 seconds."""
    producer = create_producer()
    print(f"Starting sensor reading producer — topic: {TOPIC}, sensors: 1-{NUM_SENSORS}")

    try:
        while True:
            for sensor_id in range(1, NUM_SENSORS + 1):
                reading = generate_reading(sensor_id)
                producer.send(TOPIC, value=reading)
                anomaly_label = "INJECTED ANOMALY" if reading["ground_truth_anomaly"] else "normal"
                print(
                    f"Sent sensor_id={reading['sensor_id']} "
                    f"temp={reading['temperature']} "
                    f"vib={reading['vibration']} "
                    f"[{anomaly_label}]"
                )
            producer.flush()
            print(f"Batch of {NUM_SENSORS} readings sent — sleeping {INTERVAL_SECONDS}s")
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("Shutting down producer...")
    finally:
        producer.close()
        print("Producer closed cleanly")


if __name__ == "__main__":
    main()
