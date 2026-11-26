"""Dashboard Service (порт 8085).

Контракт: api/dashboard-service.yaml (лабораторная работа №3).
Собирает пользовательское представление: дашборды и сводки. Синхронно обращается
к Metric Storage (ряд для виджета) и Analysis Service (результаты анализа),
асинхронно потребляет события anomaly.detected, notification.delivered,
notification.failed для ленты инцидентов.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from fastapi import FastAPI, Header, Query, Request
from pydantic import BaseModel, Field

from shared import http_client
from shared.app_factory import create_app
from shared.config import Config, ensure_dirs
from shared.db import Database, dumps, loads
from shared.errors import Problem
from shared.events import create_broker
from shared.idempotency import IdempotencyStore

SCHEMA = """
CREATE TABLE IF NOT EXISTS dashboards (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    widgets TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS anomaly_feed (
    event_id TEXT PRIMARY KEY,
    analysis_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    score REAL,
    reason TEXT,
    detected_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS notification_feed (
    event_id TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL,
    status TEXT NOT NULL,
    channel TEXT,
    reason TEXT,
    occurred_at TEXT NOT NULL
)
"""


class DashboardRequest(BaseModel):
    name: str = Field(min_length=1)
    widgets: list[dict] = Field(default_factory=list)


def build_app(config: Config) -> FastAPI:
    ensure_dirs(config)
    db = Database(config, SCHEMA)
    idempotency = IdempotencyStore(db)
    broker = create_broker(config)
    app = create_app(config, "dashboard-service")

    def insert_ignore(table: str, columns: list[str], values: tuple) -> None:
        placeholders = ", ".join("?" for _ in values)
        cols = ", ".join(columns)
        if db.backend == "postgres":
            sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) ON CONFLICT (event_id) DO NOTHING"
        else:
            sql = f"INSERT OR IGNORE INTO {table} ({cols}) VALUES ({placeholders})"
        db.execute(sql, values)

    def on_anomaly(event: dict) -> None:
        payload = event["payload"]
        insert_ignore("anomaly_feed",
                      ["event_id", "analysis_id", "metric_name", "score", "reason", "detected_at"],
                      (event["eventId"], payload["analysisId"], payload["metricName"],
                       payload.get("score"), payload.get("reason"), event["occurredAt"]))

    def on_notification(event: dict, status: str) -> None:
        payload = event["payload"]
        insert_ignore("notification_feed",
                      ["event_id", "incident_id", "status", "channel", "reason", "occurred_at"],
                      (event["eventId"], payload["incidentId"], status, payload.get("channel"),
                       payload.get("reason"), event["occurredAt"]))

    broker.subscribe("anomaly.detected", "dashboard", on_anomaly)
    broker.subscribe("notification.delivered", "dashboard", lambda e: on_notification(e, "DELIVERED"))
    broker.subscribe("notification.failed", "dashboard", lambda e: on_notification(e, "DELIVERY_FAILED"))

    @app.post("/api/v1/dashboards", status_code=201, tags=["public"], summary="Создать дашборд")
    async def create_dashboard(
        payload: DashboardRequest,
        request: Request,
        idempotency_key: str = Header(..., alias="Idempotency-Key"),
    ) -> dict:
        cached = idempotency.get(idempotency_key)
        if cached:
            return cached["body"]
        dashboard_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()
        db.execute("INSERT INTO dashboards (id, name, widgets, created_at) VALUES (?, ?, ?, ?)",
                   (dashboard_id, payload.name, dumps(payload.widgets), now))
        body = {"id": dashboard_id, "name": payload.name, "widgets": payload.widgets, "createdAt": now}
        idempotency.remember(idempotency_key, 201, body)
        return body

    @app.get("/api/v1/dashboards", tags=["public"], summary="Список дашбордов")
    async def list_dashboards(page: int = Query(1, ge=1), size: int = Query(20, ge=1, le=100)) -> dict:
        total = int(db.query_one("SELECT COUNT(*) AS n FROM dashboards")["n"])
        rows = db.query("SELECT id, name, widgets, created_at FROM dashboards ORDER BY created_at LIMIT ? OFFSET ?",
                        (size, (page - 1) * size))
        return {
            "page": page,
            "size": size,
            "total": total,
            "items": [{"id": r["id"], "name": r["name"], "widgets": loads(r["widgets"]),
                       "createdAt": r["created_at"]} for r in rows],
        }

    @app.get("/api/v1/dashboards/{dashboard_id}/summary", tags=["public"],
             summary="Сводка дашборда: ряд, аномалии и лента уведомлений")
    async def summary(
        dashboard_id: str,
        request: Request,
        sourceId: str = Query(...),
        metricName: str = Query(...),
        from_: datetime = Query(..., alias="from"),
        to: datetime = Query(...),
    ) -> dict:
        correlation_id = getattr(request.state, "correlation_id", None)
        dashboard = db.query_one("SELECT id, name FROM dashboards WHERE id = ?", (dashboard_id,))
        if dashboard is None:
            raise Problem(404, "Dashboard not found", "Дашборд не найден",
                          "https://example.com/errors/dashboard-not-found", correlation_id)
        series = http_client.get_json(
            f"{config.internal_url('storage')}/internal/v1/series",
            params={"sourceId": sourceId, "metricName": metricName,
                    "from": from_.isoformat(), "to": to.isoformat(), "size": 500},
            correlation_id=correlation_id,
        )
        analysis = http_client.get_json(
            f"{config.internal_url('analysis')}/internal/v1/anomalies",
            params={"sourceId": sourceId, "metricName": metricName, "size": 10},
            correlation_id=correlation_id,
        )
        anomalies_feed = db.query("SELECT * FROM anomaly_feed ORDER BY detected_at DESC LIMIT 20")
        notifications_feed = db.query("SELECT * FROM notification_feed ORDER BY occurred_at DESC LIMIT 20")
        return {
            "dashboardId": dashboard["id"],
            "name": dashboard["name"],
            "series": series,
            "analyses": analysis.get("items", []),
            "anomalyFeed": [
                {"eventId": r["event_id"], "analysisId": r["analysis_id"], "metricName": r["metric_name"],
                 "score": r["score"], "reason": r["reason"], "detectedAt": r["detected_at"]}
                for r in anomalies_feed
            ],
            "notificationFeed": [
                {"eventId": r["event_id"], "incidentId": r["incident_id"], "status": r["status"],
                 "channel": r["channel"], "reason": r["reason"], "occurredAt": r["occurred_at"]}
                for r in notifications_feed
            ],
        }

    return app


def main() -> None:
    import uvicorn

    config = Config("dashboard")
    uvicorn.run(build_app(config), host="0.0.0.0", port=config.port, log_level="info")


if __name__ == "__main__":
    main()
