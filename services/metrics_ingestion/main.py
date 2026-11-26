"""Metrics Ingestion Service (порт 8081).

Контракт: api/metrics-ingestion-service.yaml (лабораторная работа №3).
Синхронно передаёт принятые пакеты в Metric Storage (POST /internal/v1/series),
асинхронно публикует событие metrics.received для Analysis Service.
Повторяемые операции защищены заголовком Idempotency-Key.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from fastapi import FastAPI, Header, Query, Request
from pydantic import BaseModel, Field

from shared import http_client
from shared.app_factory import create_app
from shared.config import Config, ensure_dirs
from shared.db import Database
from shared.errors import Problem
from shared.events import create_broker, make_event
from shared.idempotency import IdempotencyStore

SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    endpoint TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    sample_count INTEGER NOT NULL,
    received_at TEXT NOT NULL
)
"""


class SourceRequest(BaseModel):
    name: str = Field(min_length=1)
    endpoint: str = Field(min_length=1)


class MetricSample(BaseModel):
    timestamp: datetime
    value: float


class MetricsPayload(BaseModel):
    sourceId: str
    metricName: str = Field(min_length=1)
    samples: list[MetricSample] = Field(min_length=1)


def build_app(config: Config) -> FastAPI:
    ensure_dirs(config)
    db = Database(config, SCHEMA)
    idempotency = IdempotencyStore(db)
    broker = create_broker(config)
    app = create_app(config, "metrics-ingestion-service")

    @app.post("/api/v1/sources", status_code=201, tags=["public"], summary="Подключить источник метрик")
    async def create_source(
        payload: SourceRequest,
        request: Request,
        idempotency_key: str = Header(..., alias="Idempotency-Key"),
    ) -> dict:
        cached = idempotency.get(idempotency_key)
        if cached:
            return cached["body"]
        existing = db.query_one("SELECT id, name, endpoint, status FROM sources WHERE name = ?", (payload.name,))
        if existing:
            raise Problem(409, "Source already exists", f"Источник '{payload.name}' уже подключён",
                          "https://example.com/errors/source-conflict",
                          getattr(request.state, "correlation_id", None))
        source_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()
        db.execute(
            "INSERT INTO sources (id, name, endpoint, status, created_at) VALUES (?, ?, ?, ?, ?)",
            (source_id, payload.name, payload.endpoint, "ACTIVE", now),
        )
        body = {"id": source_id, "name": payload.name, "endpoint": payload.endpoint,
                "status": "ACTIVE", "createdAt": now}
        idempotency.remember(idempotency_key, 201, body)
        return body

    @app.get("/api/v1/sources", tags=["public"], summary="Список источников метрик")
    async def list_sources(page: int = Query(1, ge=1), size: int = Query(20, ge=1, le=100)) -> dict:
        total = int(db.query_one("SELECT COUNT(*) AS n FROM sources")["n"])
        rows = db.query("SELECT id, name, endpoint, status, created_at FROM sources ORDER BY created_at LIMIT ? OFFSET ?",
                        (size, (page - 1) * size))
        return {
            "page": page,
            "size": size,
            "total": total,
            "items": [
                {"id": r["id"], "name": r["name"], "endpoint": r["endpoint"],
                 "status": r["status"], "createdAt": r["created_at"]}
                for r in rows
            ],
        }

    @app.post("/api/v1/metrics", status_code=202, tags=["public"], summary="Принять пакет метрик")
    async def ingest_metrics(
        payload: MetricsPayload,
        request: Request,
        idempotency_key: str = Header(..., alias="Idempotency-Key"),
    ) -> dict:
        cached = idempotency.get(idempotency_key)
        if cached:
            return cached["body"]
        correlation_id = getattr(request.state, "correlation_id", None)
        source = db.query_one("SELECT id, status FROM sources WHERE id = ?", (payload.sourceId,))
        if source is None:
            raise Problem(404, "Source not found", "Источник метрик не найден",
                          "https://example.com/errors/source-not-found", correlation_id)
        if source["status"] != "ACTIVE":
            raise Problem(409, "Source disabled", "Источник отключён",
                          "https://example.com/errors/source-disabled", correlation_id)

        # Синхронный шаг: передача ряда в Metric Storage по контракту.
        series_body = {
            "sourceId": payload.sourceId,
            "metricName": payload.metricName,
            "samples": [{"timestamp": s.timestamp.isoformat(), "value": s.value} for s in payload.samples],
        }
        try:
            http_client.post_json(f"{config.internal_url('storage')}/internal/v1/series",
                                  series_body, correlation_id=correlation_id)
        except http_client.ServiceCallError as exc:
            raise Problem(503, "Storage unavailable", f"Хранилище метрик недоступно: {exc.detail}",
                          "https://example.com/errors/storage-unavailable", correlation_id) from exc

        batch_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()
        db.execute(
            "INSERT INTO batches (id, source_id, metric_name, sample_count, received_at) VALUES (?, ?, ?, ?, ?)",
            (batch_id, payload.sourceId, payload.metricName, len(payload.samples), now),
        )

        # Асинхронный шаг: событие для Analysis Service.
        first_ts = min(s.timestamp for s in payload.samples).isoformat()
        last_ts = max(s.timestamp for s in payload.samples).isoformat()
        broker.publish("metrics.received", make_event("metrics.received", {
            "sourceId": payload.sourceId,
            "metricName": payload.metricName,
            "batchId": batch_id,
            "from": first_ts,
            "to": last_ts,
        }, correlation_id))

        body = {"batchId": batch_id, "accepted": len(payload.samples), "receivedAt": now}
        idempotency.remember(idempotency_key, 202, body)
        return body

    return app


def main() -> None:
    import uvicorn

    config = Config("ingestion")
    uvicorn.run(build_app(config), host="0.0.0.0", port=config.port, log_level="info")


if __name__ == "__main__":
    main()
