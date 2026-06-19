"""FastAPI application for querying detected anomalies and evaluation metrics."""

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

import db


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Verify database connectivity on startup."""
    try:
        conn = db.get_connection()
        conn.close()
        print("Database connection verified on startup")
    except Exception as exc:
        raise RuntimeError(f"Failed to connect to database on startup: {exc}") from exc
    yield


app = FastAPI(
    title="Kafka Anomaly Detection API",
    description="Query detected anomalies and model evaluation metrics",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    """Return service health status."""
    return {"status": "ok"}


@app.get("/anomalies/recent")
def recent_anomalies(limit: int = Query(default=20, ge=1, le=100)):
    """Return recently detected anomalies."""
    return db.get_recent_anomalies(limit=limit)


@app.get("/anomalies/stats")
def anomaly_stats():
    """Return summary statistics for readings and anomalies."""
    return db.get_anomaly_stats()


@app.get("/anomalies/evaluation")
def anomaly_evaluation():
    """Return precision, recall, and F1 comparing model predictions to ground truth."""
    return db.get_evaluation_metrics()
