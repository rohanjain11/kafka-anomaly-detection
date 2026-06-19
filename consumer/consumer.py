"""Kafka consumer that scores sensor readings and persists anomalies to PostgreSQL."""

import json
import os
import time

import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from model import score_reading, train_baseline_model

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC = "sensor-readings"
CONSUMER_GROUP = "anomaly-detector"

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "anomaly_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "anomaly_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "anomaly_pass")

MAX_RETRIES = 5
RETRY_DELAY_SECONDS = 3


def create_consumer() -> KafkaConsumer:
    """Create and return a configured Kafka consumer with retry logic.

    Returns:
        Connected KafkaConsumer subscribed to sensor-readings topic.

    Raises:
        NoBrokersAvailable: If broker is unreachable after all retries.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Connecting to Kafka broker at {KAFKA_BROKER} (attempt {attempt}/{MAX_RETRIES})...")
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=CONSUMER_GROUP,
                auto_offset_reset="earliest",
                value_deserializer=lambda m: m.decode("utf-8"),
            )
            print(f"Subscribed to topic '{TOPIC}' as group '{CONSUMER_GROUP}'")
            return consumer
        except NoBrokersAvailable:
            if attempt == MAX_RETRIES:
                print(f"Failed to connect to Kafka after {MAX_RETRIES} attempts")
                raise
            print(f"Kafka not ready, retrying in {RETRY_DELAY_SECONDS}s...")
            time.sleep(RETRY_DELAY_SECONDS)

    raise NoBrokersAvailable()


def get_db_connection():
    """Return a psycopg2 connection to PostgreSQL with retry logic.

    Returns:
        Active psycopg2 connection.

    Raises:
        psycopg2.OperationalError: If database is unreachable after all retries.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Connecting to PostgreSQL at {POSTGRES_HOST} (attempt {attempt}/{MAX_RETRIES})...")
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
            print(f"Connected to PostgreSQL database '{POSTGRES_DB}'")
            return conn
        except psycopg2.OperationalError as exc:
            if attempt == MAX_RETRIES:
                print(f"Failed to connect to PostgreSQL after {MAX_RETRIES} attempts: {exc}")
                raise
            print(f"PostgreSQL not ready, retrying in {RETRY_DELAY_SECONDS}s...")
            time.sleep(RETRY_DELAY_SECONDS)

    raise psycopg2.OperationalError("Could not connect to PostgreSQL")


def insert_reading_log(conn, reading: dict, is_anomaly: bool, anomaly_score: float) -> None:
    """Insert a processed reading into the readings_log audit table.

    Args:
        conn: Active psycopg2 connection.
        reading: Parsed sensor reading dict from Kafka.
        is_anomaly: Model prediction flag.
        anomaly_score: Isolation Forest decision function score.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO readings_log
                (sensor_id, timestamp, temperature, vibration, anomaly_score,
                 is_anomaly, ground_truth_anomaly)
            VALUES (%s, to_timestamp(%s), %s, %s, %s, %s, %s)
            """,
            (
                reading["sensor_id"],
                reading["timestamp"],
                reading["temperature"],
                reading["vibration"],
                anomaly_score,
                is_anomaly,
                reading.get("ground_truth_anomaly", False),
            ),
        )
    conn.commit()


def insert_anomaly(conn, reading: dict, anomaly_score: float) -> None:
    """Insert a detected anomaly into the anomalies table.

    Args:
        conn: Active psycopg2 connection.
        reading: Parsed sensor reading dict from Kafka.
        anomaly_score: Isolation Forest decision function score.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO anomalies
                (sensor_id, timestamp, temperature, vibration, anomaly_score,
                 ground_truth_anomaly)
            VALUES (%s, to_timestamp(%s), %s, %s, %s, %s)
            """,
            (
                reading["sensor_id"],
                reading["timestamp"],
                reading["temperature"],
                reading["vibration"],
                anomaly_score,
                reading.get("ground_truth_anomaly", False),
            ),
        )
    conn.commit()


def main() -> None:
    """Train model, consume Kafka messages, score readings, and persist results."""
    print("Training baseline anomaly detection model...")
    model = train_baseline_model()

    consumer = create_consumer()
    conn = get_db_connection()
    print("Consumer ready — waiting for sensor readings...")

    try:
        for message in consumer:
            try:
                reading = json.loads(message.value)
            except json.JSONDecodeError as exc:
                print(f"Skipping malformed message: {exc}")
                continue

            try:
                temperature = float(reading["temperature"])
                vibration = float(reading["vibration"])
            except (KeyError, TypeError, ValueError) as exc:
                print(f"Skipping invalid reading fields: {exc}")
                continue

            # Score using ONLY temperature and vibration — never ground_truth_anomaly.
            # That field exists only for offline evaluation via the API to avoid data leakage.
            is_anomaly, anomaly_score = score_reading(model, temperature, vibration)
            is_anomaly = bool(is_anomaly)

            insert_reading_log(conn, reading, is_anomaly, anomaly_score)

            if is_anomaly:
                insert_anomaly(conn, reading, anomaly_score)
                print(
                    f"*** ANOMALY DETECTED *** sensor_id={reading['sensor_id']} "
                    f"temp={temperature} vib={vibration} score={anomaly_score:.4f}"
                )
            else:
                print(
                    f"Scored sensor_id={reading['sensor_id']} "
                    f"temp={temperature} vib={vibration} score={anomaly_score:.4f} [normal]"
                )
    except KeyboardInterrupt:
        print("Shutting down consumer...")
    finally:
        consumer.close()
        conn.close()
        print("Consumer closed cleanly")


if __name__ == "__main__":
    main()
