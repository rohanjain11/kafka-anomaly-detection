"""Database access layer for the anomaly detection API."""

import os
import time

import psycopg2
from psycopg2.extras import RealDictCursor

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "anomaly_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "anomaly_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "anomaly_pass")

MAX_RETRIES = 5
RETRY_DELAY_SECONDS = 3


def get_connection():
    """Return a psycopg2 connection to PostgreSQL with retry logic.

    Returns:
        Active psycopg2 connection.

    Raises:
        psycopg2.OperationalError: If database is unreachable after all retries.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
            return conn
        except psycopg2.OperationalError:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_DELAY_SECONDS)

    raise psycopg2.OperationalError("Could not connect to PostgreSQL")


def get_recent_anomalies(limit: int = 20) -> list[dict]:
    """Query recent detected anomalies ordered by detection time.

    Args:
        limit: Maximum number of anomalies to return.

    Returns:
        List of anomaly records as dicts.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, sensor_id, timestamp, temperature, vibration,
                       anomaly_score, detected_at, ground_truth_anomaly
                FROM anomalies
                ORDER BY detected_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
            return [dict(row) for row in rows]
    finally:
        conn.close()


def get_anomaly_stats() -> dict:
    """Return summary statistics for processed readings and detected anomalies.

    Returns:
        Dict with total_readings, total_anomalies, anomaly_rate, and
        per-sensor breakdown.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS count FROM readings_log")
            total_readings = cur.fetchone()["count"]

            cur.execute("SELECT COUNT(*) AS count FROM anomalies")
            total_anomalies = cur.fetchone()["count"]

            anomaly_rate = (
                round((total_anomalies / total_readings) * 100, 2)
                if total_readings > 0
                else 0.0
            )

            cur.execute(
                """
                SELECT sensor_id,
                       COUNT(*) AS anomaly_count
                FROM anomalies
                GROUP BY sensor_id
                ORDER BY sensor_id
                """
            )
            by_sensor = {row["sensor_id"]: row["anomaly_count"] for row in cur.fetchall()}

            return {
                "total_readings": total_readings,
                "total_anomalies": total_anomalies,
                "anomaly_rate_percent": anomaly_rate,
                "anomalies_by_sensor": by_sensor,
            }
    finally:
        conn.close()


def get_evaluation_metrics() -> dict:
    """Compare model predictions against injected ground truth labels.

    Computes precision, recall, and F1 by treating is_anomaly (model prediction)
    as the predicted label and ground_truth_anomaly as the true label.

    Returns:
        Dict with precision, recall, f1_score, and confusion matrix counts.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    SUM(CASE WHEN is_anomaly AND ground_truth_anomaly THEN 1 ELSE 0 END) AS true_positives,
                    SUM(CASE WHEN is_anomaly AND NOT ground_truth_anomaly THEN 1 ELSE 0 END) AS false_positives,
                    SUM(CASE WHEN NOT is_anomaly AND ground_truth_anomaly THEN 1 ELSE 0 END) AS false_negatives,
                    SUM(CASE WHEN NOT is_anomaly AND NOT ground_truth_anomaly THEN 1 ELSE 0 END) AS true_negatives,
                    COUNT(*) AS total
                FROM readings_log
                """
            )
            row = cur.fetchone()

            tp = row["true_positives"] or 0
            fp = row["false_positives"] or 0
            fn = row["false_negatives"] or 0
            tn = row["true_negatives"] or 0
            total = row["total"] or 0

            precision = round(tp / (tp + fp), 4) if (tp + fp) > 0 else 0.0
            recall = round(tp / (tp + fn), 4) if (tp + fn) > 0 else 0.0
            f1 = (
                round(2 * precision * recall / (precision + recall), 4)
                if (precision + recall) > 0
                else 0.0
            )

            return {
                "precision": precision,
                "recall": recall,
                "f1_score": f1,
                "confusion_matrix": {
                    "true_positives": tp,
                    "false_positives": fp,
                    "false_negatives": fn,
                    "true_negatives": tn,
                },
                "total_readings_evaluated": total,
            }
    finally:
        conn.close()
