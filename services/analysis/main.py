"""Analysis Service (порт 8083).

Контракт: api/analysis-service.yaml (лабораторная работа №3).
Потребляет событие metrics.received (обновляет модель метрики), по запросу запускает
анализ: синхронно читает ряд из Metric Storage, обнаруживает аномалии (rolling z-score),
строит прогноз и публикует события anomaly.detected / forecast.created.
"""
from __future__ import annotations

import threading
import uuid
from datetime import datetime

from fastapi import FastAPI, Header, Query, Request
from pydantic import BaseModel, Field

from shared import http_client, ml
from shared.app_factory import create_app
from shared.config import Config, ensure_dirs
from shared.db import Database, dumps, loads
from shared.errors import Problem
from shared.events import create_broker, make_event
from shared.idempotency import IdempotencyStore

SCHEMA = """
CREATE TABLE IF NOT EXISTS analyses (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    from_ts TEXT NOT NULL,
    to_ts TEXT NOT NULL,
    forecast_horizon INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    result TEXT,
    error TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS models (
    source_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    artifact TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (source_id, metric_name)
);
CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY
)
"""

WINDOW = 20
THRESHOLD = 3.0


class AnalysisRequest(BaseModel):
    sourceId: str
    metricName: str = Field(min_length=1)
    from_: datetime = Field(alias="from")
    to: datetime
    forecastHorizon: int = Field(default=0, ge=0, le=1000)


def build_app(config: Config) -> FastAPI:
    ensure_dirs(config)
    db = Database(config, SCHEMA)
    idempotency = IdempotencyStore(db)
    broker = create_broker(config)
    app = create_app(config, "analysis-service")

    def fetch_series(source_id: str, metric_name: str, start: datetime, end: datetime,
                     correlation_id: str | None) -> tuple[list[str], list[float]]:
        data = http_client.get_json(
            f"{config.internal_url('storage')}/internal/v1/series",
            params={"sourceId": source_id, "metricName": metric_name,
                    "from": start.isoformat(), "to": end.isoformat(), "size": 5000},
            correlation_id=correlation_id,
        )
        samples = data.get("samples", [])
        return [s["timestamp"] for s in samples], [float(s["value"]) for s in samples]

    def run_analysis(analysis_id: str, source_id: str, metric_name: str,
                     start: datetime, end: datetime, horizon: int,
                     correlation_id: str | None) -> None:
        try:
            timestamps, values = fetch_series(source_id, metric_name, start, end, correlation_id)
            if len(values) < WINDOW:
                status, result = "COMPLETED", {"anomalies": [], "forecast": [],
                                               "note": "Недостаточно данных для анализа"}
            else:
                anomalies = [a.to_dict() for a in ml.detect_anomalies(timestamps, values, WINDOW, THRESHOLD)]
                forecast_points = ml.forecast(values, horizon)
                result = {"anomalies": anomalies, "forecast": forecast_points,
                          "model": ml.train_model(values, WINDOW, THRESHOLD)}
                status = "COMPLETED"
            db.execute("UPDATE analyses SET status = ?, result = ? WHERE id = ?",
                       (status, dumps(result), analysis_id))
            if result.get("anomalies"):
                worst = max(result["anomalies"], key=lambda a: a["score"])
                broker.publish("anomaly.detected", make_event("anomaly.detected", {
                    "analysisId": analysis_id,
                    "sourceId": source_id,
                    "metricName": metric_name,
                    "timestamp": worst["timestamp"],
                    "value": worst["value"],
                    "score": worst["score"],
                    "reason": worst["reason"],
                    "anomalies": result["anomalies"],
                }, correlation_id))
            if horizon and result.get("forecast"):
                broker.publish("forecast.created", make_event("forecast.created", {
                    "analysisId": analysis_id,
                    "sourceId": source_id,
                    "metricName": metric_name,
                    "points": result["forecast"],
                }, correlation_id))
        except Exception as exc:  # noqa: BLE001
            db.execute("UPDATE analyses SET status = 'FAILED', error = ? WHERE id = ?",
                       (str(exc), analysis_id))

    def on_metrics_received(event: dict) -> None:
        """Асинхронный потребитель: обновляет модель метрики по новому пакету."""
        event_id = event["eventId"]
        if db.query_one("SELECT event_id FROM processed_events WHERE event_id = ?", (event_id,)):
            return
        payload = event["payload"]
        timestamps, values = fetch_series(payload["sourceId"], payload["metricName"],
                                          datetime.fromisoformat(payload["from"]),
                                          datetime.fromisoformat(payload["to"]),
                                          event.get("correlationId"))
        history = db.query_one(
            "SELECT artifact FROM models WHERE source_id = ? AND metric_name = ?",
            (payload["sourceId"], payload["metricName"]),
        )
        prior = loads(history["artifact"]).get("samples", []) if history else []
        combined = (prior + values)[-500:]
        artifact = ml.train_model(combined, WINDOW, THRESHOLD)
        artifact["samples"] = combined
        if db.backend == "postgres":
            db.execute(
                "INSERT INTO models (source_id, metric_name, artifact, updated_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(source_id, metric_name) DO UPDATE SET artifact = excluded.artifact, "
                "updated_at = excluded.updated_at",
                (payload["sourceId"], payload["metricName"], dumps(artifact), datetime.utcnow().isoformat()),
            )
        else:
            db.execute(
                "INSERT OR REPLACE INTO models (source_id, metric_name, artifact, updated_at) VALUES (?, ?, ?, ?)",
                (payload["sourceId"], payload["metricName"], dumps(artifact), datetime.utcnow().isoformat()),
            )
        db.execute("INSERT INTO processed_events (event_id) VALUES (?)", (event_id,))

    broker.subscribe("metrics.received", "analysis", on_metrics_received)

    @app.post("/api/v1/analyses", status_code=202, tags=["public"], summary="Запустить анализ метрики")
    async def start_analysis(
        payload: AnalysisRequest,
        request: Request,
        idempotency_key: str = Header(..., alias="Idempotency-Key"),
    ) -> dict:
        cached = idempotency.get(idempotency_key)
        if cached:
            return cached["body"]
        correlation_id = getattr(request.state, "correlation_id", None)
        if payload.from_ > payload.to:
            raise Problem(400, "Invalid range", "Параметр from должен быть не позже to",
                          "https://example.com/errors/invalid-range", correlation_id)
        running = db.query_one(
            "SELECT id FROM analyses WHERE source_id = ? AND metric_name = ? AND status = 'RUNNING'",
            (payload.sourceId, payload.metricName),
        )
        if running:
            raise Problem(409, "Analysis already running", "Анализ этой метрики уже выполняется",
                          "https://example.com/errors/analysis-conflict", correlation_id)
        analysis_id = str(uuid.uuid4())
        db.execute(
            "INSERT INTO analyses (id, source_id, metric_name, from_ts, to_ts, forecast_horizon, status, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, 'RUNNING', ?)",
            (analysis_id, payload.sourceId, payload.metricName, payload.from_.isoformat(),
             payload.to.isoformat(), payload.forecastHorizon, datetime.utcnow().isoformat()),
        )
        thread = threading.Thread(
            target=run_analysis,
            args=(analysis_id, payload.sourceId, payload.metricName,
                  payload.from_, payload.to, payload.forecastHorizon, correlation_id),
            daemon=True,
        )
        thread.start()
        body = {"analysisId": analysis_id, "status": "RUNNING"}
        idempotency.remember(idempotency_key, 202, body)
        return body

    @app.get("/internal/v1/anomalies", tags=["internal"], summary="Результаты анализа (для сервисов)")
    async def anomalies(
        sourceId: str = Query(...),
        metricName: str = Query(...),
        page: int = Query(1, ge=1),
        size: int = Query(20, ge=1, le=100),
    ) -> dict:
        total = int(db.query_one(
            "SELECT COUNT(*) AS n FROM analyses WHERE source_id = ? AND metric_name = ?",
            (sourceId, metricName))["n"])
        rows = db.query(
            "SELECT id, status, result, error, from_ts, to_ts, forecast_horizon, created_at FROM analyses "
            "WHERE source_id = ? AND metric_name = ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (sourceId, metricName, size, (page - 1) * size),
        )
        items = []
        for row in rows:
            result = loads(row["result"]) or {}
            items.append({
                "analysisId": row["id"],
                "status": row["status"],
                "from": row["from_ts"],
                "to": row["to_ts"],
                "forecastHorizon": row["forecast_horizon"],
                "anomalies": result.get("anomalies", []),
                "forecast": result.get("forecast", []),
                "error": row["error"],
                "createdAt": row["created_at"],
            })
        return {"page": page, "size": size, "total": total, "items": items}

    return app


def main() -> None:
    import uvicorn

    config = Config("analysis")
    uvicorn.run(build_app(config), host="0.0.0.0", port=config.port, log_level="info")


if __name__ == "__main__":
    main()
