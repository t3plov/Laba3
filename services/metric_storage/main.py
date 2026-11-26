"""Metric Storage Service (порт 8082).

Контракт: api/metric-storage-service.yaml (лабораторная работа №3).
Собственное хранилище: PostgreSQL (docker-compose) или SQLite (локально).
Синхронные взаимодействия: приём ряда от Ingestion, выдача ряда Analysis и Dashboard.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from fastapi import FastAPI, Query, Request
from pydantic import BaseModel, Field

from shared.app_factory import create_app
from shared.config import Config, ensure_dirs
from shared.db import Database
from shared.errors import Problem

SCHEMA = """
CREATE TABLE IF NOT EXISTS samples (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    ts TEXT NOT NULL,
    value REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_series ON samples (source_id, metric_name, ts);
CREATE UNIQUE INDEX IF NOT EXISTS idx_series_unique ON samples (source_id, metric_name, ts)
"""


class MetricSample(BaseModel):
    timestamp: datetime
    value: float


class SeriesPayload(BaseModel):
    sourceId: str
    metricName: str = Field(min_length=1)
    samples: list[MetricSample] = Field(min_length=1)


def build_app(config: Config) -> FastAPI:
    ensure_dirs(config)
    db = Database(config, SCHEMA)
    app = create_app(config, "metric-storage-service")

    def store_series(payload: SeriesPayload) -> int:
        inserted = 0
        for sample in payload.samples:
            ts = sample.timestamp.isoformat()
            existing = db.query_one(
                "SELECT id FROM samples WHERE source_id = ? AND metric_name = ? AND ts = ?",
                (payload.sourceId, payload.metricName, ts),
            )
            if existing is None:
                db.execute(
                    "INSERT INTO samples (id, source_id, metric_name, ts, value) VALUES (?, ?, ?, ?, ?)",
                    (str(uuid.uuid4()), payload.sourceId, payload.metricName, ts, sample.value),
                )
                inserted += 1
        return inserted

    @app.post("/internal/v1/series", status_code=202, tags=["internal"],
              summary="Сохранить временной ряд (вызывается Metrics Ingestion)")
    async def internal_series(payload: SeriesPayload) -> dict:
        inserted = store_series(payload)
        return {"accepted": inserted, "metricName": payload.metricName, "sourceId": payload.sourceId}

    def read_series(source_id: str, metric_name: str, start: datetime, end: datetime,
                    page: int, size: int) -> dict:
        if start > end:
            raise Problem(400, "Invalid range", "Параметр from должен быть не позже to",
                          "https://example.com/errors/invalid-range")
        known = db.query_one(
            "SELECT COUNT(*) AS n FROM samples WHERE source_id = ? AND metric_name = ?",
            (source_id, metric_name),
        )
        if not known or known["n"] == 0:
            raise Problem(404, "Series not found", "Ряд метрики не найден",
                          "https://example.com/errors/series-not-found")
        total_row = db.query_one(
            "SELECT COUNT(*) AS n FROM samples WHERE source_id = ? AND metric_name = ? AND ts >= ? AND ts <= ?",
            (source_id, metric_name, start.isoformat(), end.isoformat()),
        )
        total = int(total_row["n"]) if total_row else 0
        offset = (page - 1) * size
        rows = db.query(
            "SELECT ts, value FROM samples WHERE source_id = ? AND metric_name = ? AND ts >= ? AND ts <= ? "
            "ORDER BY ts LIMIT ? OFFSET ?",
            (source_id, metric_name, start.isoformat(), end.isoformat(), size, offset),
        )
        return {
            "sourceId": source_id,
            "metricName": metric_name,
            "page": page,
            "size": size,
            "total": total,
            "samples": [{"timestamp": r["ts"], "value": r["value"]} for r in rows],
        }

    @app.get("/internal/v1/series", tags=["internal"], summary="Получить временной ряд (для сервисов)")
    async def internal_read(
        sourceId: str = Query(...),
        metricName: str = Query(...),
        from_: datetime = Query(..., alias="from"),
        to: datetime = Query(...),
        page: int = Query(1, ge=1),
        size: int = Query(1000, ge=1, le=5000),
    ) -> dict:
        return read_series(sourceId, metricName, from_, to, page, size)

    @app.get("/api/v1/series", tags=["public"], summary="Получить временной ряд (для клиента через Gateway)")
    async def public_read(
        request: Request,
        sourceId: str = Query(...),
        metricName: str = Query(...),
        from_: datetime = Query(..., alias="from"),
        to: datetime = Query(...),
        page: int = Query(1, ge=1),
        size: int = Query(100, ge=1, le=1000),
    ) -> dict:
        return read_series(sourceId, metricName, from_, to, page, size)

    return app


def main() -> None:
    import uvicorn

    config = Config("storage")
    uvicorn.run(build_app(config), host="0.0.0.0", port=config.port, log_level="info")


if __name__ == "__main__":
    main()
