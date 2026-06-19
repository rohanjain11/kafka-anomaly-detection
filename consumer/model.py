"""Isolation Forest model for anomaly detection on sensor readings."""

import random

import numpy as np
from sklearn.ensemble import IsolationForest


def train_baseline_model() -> IsolationForest:
    """Train an Isolation Forest on synthetic normal-range sensor data.

    Generates 500 normal readings (temperature 60-80, vibration 0.1-0.5)
    representing baseline operating conditions before deployment.

    Returns:
        Fitted IsolationForest model.
    """
    random.seed(42)
    np.random.seed(42)

    training_data = []
    for _ in range(500):
        temperature = random.uniform(60, 80)
        vibration = random.uniform(0.1, 0.5)
        training_data.append([temperature, vibration])

    model = IsolationForest(contamination=0.05, random_state=42)
    model.fit(training_data)
    print("Baseline Isolation Forest model trained on 500 normal readings")
    return model


def score_reading(model: IsolationForest, temperature: float, vibration: float) -> tuple[bool, float]:
    """Score a single reading for anomalous behavior.

    Args:
        model: Fitted IsolationForest model.
        temperature: Sensor temperature reading.
        vibration: Sensor vibration reading.

    Returns:
        Tuple of (is_anomaly, anomaly_score) where lower/negative
        decision_function scores indicate more anomalous readings.
    """
    features = np.array([[temperature, vibration]])
    prediction = model.predict(features)[0]
    anomaly_score = float(model.decision_function(features)[0])
    is_anomaly = prediction == -1
    return is_anomaly, anomaly_score
