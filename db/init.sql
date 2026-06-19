-- Anomalies detected by the Isolation Forest model
CREATE TABLE IF NOT EXISTS anomalies (
    id SERIAL PRIMARY KEY,
    sensor_id INT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    temperature FLOAT NOT NULL,
    vibration FLOAT NOT NULL,
    anomaly_score FLOAT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ground_truth_anomaly BOOLEAN NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_anomalies_sensor_id ON anomalies (sensor_id);
CREATE INDEX IF NOT EXISTS idx_anomalies_timestamp ON anomalies (timestamp);

-- Full audit log of every processed reading
CREATE TABLE IF NOT EXISTS readings_log (
    id SERIAL PRIMARY KEY,
    sensor_id INT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    temperature FLOAT NOT NULL,
    vibration FLOAT NOT NULL,
    anomaly_score FLOAT NOT NULL,
    is_anomaly BOOLEAN NOT NULL,
    ground_truth_anomaly BOOLEAN NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_readings_log_sensor_id ON readings_log (sensor_id);
CREATE INDEX IF NOT EXISTS idx_readings_log_timestamp ON readings_log (timestamp);
