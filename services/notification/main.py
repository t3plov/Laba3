"""Notification Service (порт 8084).

Контракт: api/notification-service.yaml (лабораторная работа №3).
Потребляет события anomaly.detected / forecast.created, создаёт инцидент и доставляет
уведомление во внешний канал (заглушка WireMock). Сквозной сценарий со сбоем:

1. доставка в основной канал падает — выполняются повторные попытки;
2. компенсирующее действие — переключение на резервный канал;
3. если и резервный канал недоступен, инцидент получает статус DELIVERY_FAILED
   и публикуется событие notification.failed.

Потребитель идемпотентен: обработанные eventId запоминаются, повторная доставка
события игнорируется.
"""
from __future__ import annotations

import time
import uuid
from datetime import datetime

import httpx
from fastapi import FastAPI, Header, Query, Request
from pydantic import BaseModel, Field

from shared.app_factory import create_app
from shared.config import Config, ensure_dirs
from shared.db import Database
from shared.errors import Problem
from shared.events import create_broker, make_event
from shared.idempotency import IdempotencyStore

SCHEMA = """
CREATE TABLE IF NOT EXISTS rules (
    id TEXT PRIMARY KEY,
    metric_name TEXT NOT NULL,
    condition TEXT NOT NULL,
    channel TEXT NOT NULL,
    fallback_channel TEXT,
    threshold REAL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS incidents (
    id TEXT PRIMARY KEY,
    rule_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    status TEXT NOT NULL,
    channel_used TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    detail TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS delivery_attempts (
    id TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    url TEXT NOT NULL,
    status TEXT NOT NULL,
    error TEXT,
    attempted_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY
)
"""

MAX_ATTEMPTS = 3
RETRY_DELAY = 0.2


class RuleRequest(BaseModel):
    metricName: str = Field(min_length=1)
    condition: str = Field(pattern="^(ANOMALY|FORECAST_THRESHOLD)$")
    channel: str = Field(pattern="^(EMAIL|WEBHOOK|TELEGRAM)$")
    fallbackChannel: str | None = Field(default=None, pattern="^(EMAIL|WEBHOOK|TELEGRAM)$")
    threshold: float | None = None


def build_app(config: Config) -> FastAPI:
    ensure_dirs(config)
    db = Database(config, SCHEMA)
    idempotency = IdempotencyStore(db)
    broker = create_broker(config)
    app = create_app(config, "notification-service")

    def channel_url(channel: str, fallback: bool = False) -> str:
        base = config.fallback_channel_url if fallback else config.channel_url
        return f"{base.rstrip('/')}/send/{channel.lower()}"

    def attempt_delivery(incident_id: str, channel: str, fallback: bool, message: dict) -> tuple[bool, str]:
        url = channel_url(channel, fallback)
        error = ""
        for _ in range(MAX_ATTEMPTS):
            try:
                with httpx.Client(timeout=3.0) as client:
                    response = client.post(url, json=message)
                if response.status_code < 400:
                    db.execute(
                        "INSERT INTO delivery_attempts (id, incident_id, channel, url, status, error, attempted_at) "
                        "VALUES (?, ?, ?, ?, 'OK', '', ?)",
                        (str(uuid.uuid4()), incident_id, channel, url, datetime.utcnow().isoformat()),
                    )
                    return True, ""
                error = f"HTTP {response.status_code}"
            except Exception as exc:  # noqa: BLE001 - канал недоступен
                error = str(exc)
            db.execute(
                "INSERT INTO delivery_attempts (id, incident_id, channel, url, status, error, attempted_at) "
                "VALUES (?, ?, ?, ?, 'FAILED', ?, ?)",
                (str(uuid.uuid4()), incident_id, channel, url, error, datetime.utcnow().isoformat()),
            )
            time.sleep(RETRY_DELAY)
        return False, error

    def handle_event(event: dict, condition: str) -> None:
        event_id = event["eventId"]
        if db.query_one("SELECT event_id FROM processed_events WHERE event_id = ?", (event_id,)):
            return  # повторная доставка брокером — игнорируем
        payload = event["payload"]
        rules = db.query("SELECT * FROM rules WHERE metric_name = ? AND condition = ?",
                         (payload["metricName"], condition))
        db.execute("INSERT INTO processed_events (event_id) VALUES (?)", (event_id,))
        for rule in rules:
            if condition == "FORECAST_THRESHOLD" and rule["threshold"] is not None:
                points = payload.get("points", [])
                if not points or max(points) < float(rule["threshold"]):
                    continue
            incident_id = str(uuid.uuid4())
            now = datetime.utcnow().isoformat()
            db.execute(
                "INSERT INTO incidents (id, rule_id, event_id, metric_name, status, channel_used, attempts, "
                "detail, created_at, updated_at) VALUES (?, ?, ?, ?, 'PENDING', NULL, 0, '', ?, ?)",
                (incident_id, rule["id"], event_id, payload["metricName"], now, now),
            )
            message = {
                "incidentId": incident_id,
                "metricName": payload["metricName"],
                "eventType": event["eventType"],
                "score": payload.get("score"),
                "correlationId": event.get("correlationId"),
            }
            ok, error = attempt_delivery(incident_id, rule["channel"], False, message)
            attempts = MAX_ATTEMPTS
            channel_used = rule["channel"]
            if not ok and rule["fallback_channel"]:
                # Компенсирующее действие: переключение на резервный канал.
                ok, error = attempt_delivery(incident_id, rule["fallback_channel"], True, message)
                attempts += MAX_ATTEMPTS
                channel_used = rule["fallback_channel"] if ok else rule["channel"]
            now = datetime.utcnow().isoformat()
            if ok:
                status = "DELIVERED"
                broker.publish("notification.delivered", make_event("notification.delivered", {
                    "incidentId": incident_id,
                    "channel": channel_used,
                    "metricName": payload["metricName"],
                }, event.get("correlationId")))
            else:
                status = "DELIVERY_FAILED"
                broker.publish("notification.failed", make_event("notification.failed", {
                    "incidentId": incident_id,
                    "reason": error or "delivery failed",
                    "metricName": payload["metricName"],
                }, event.get("correlationId")))
            db.execute(
                "UPDATE incidents SET status = ?, channel_used = ?, attempts = ?, detail = ?, updated_at = ? "
                "WHERE id = ?",
                (status, channel_used, attempts, error, now, incident_id),
            )

    broker.subscribe("anomaly.detected", "notification", lambda event: handle_event(event, "ANOMALY"))
    broker.subscribe("forecast.created", "notification", lambda event: handle_event(event, "FORECAST_THRESHOLD"))

    @app.post("/api/v1/notification-rules", status_code=201, tags=["public"],
              summary="Создать правило уведомления")
    async def create_rule(
        payload: RuleRequest,
        request: Request,
        idempotency_key: str = Header(..., alias="Idempotency-Key"),
    ) -> dict:
        cached = idempotency.get(idempotency_key)
        if cached:
            return cached["body"]
        correlation_id = getattr(request.state, "correlation_id", None)
        existing = db.query_one(
            "SELECT id FROM rules WHERE metric_name = ? AND condition = ? AND channel = ?",
            (payload.metricName, payload.condition, payload.channel),
        )
        if existing:
            raise Problem(409, "Rule already exists", "Такое правило уже существует",
                          "https://example.com/errors/rule-conflict", correlation_id)
        rule_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()
        db.execute(
            "INSERT INTO rules (id, metric_name, condition, channel, fallback_channel, threshold, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (rule_id, payload.metricName, payload.condition, payload.channel,
             payload.fallbackChannel, payload.threshold, now),
        )
        body = {"id": rule_id, "metricName": payload.metricName, "condition": payload.condition,
                "channel": payload.channel, "fallbackChannel": payload.fallbackChannel,
                "threshold": payload.threshold, "createdAt": now}
        idempotency.remember(idempotency_key, 201, body)
        return body

    @app.get("/api/v1/incidents", tags=["public"], summary="История инцидентов")
    async def list_incidents(
        status: str | None = Query(default=None, pattern="^(OPEN|PENDING|DELIVERED|DELIVERY_FAILED)$"),
        page: int = Query(1, ge=1),
        size: int = Query(20, ge=1, le=100),
    ) -> dict:
        where = "WHERE status = ?" if status else ""
        params: list = [status] if status else []
        total = int(db.query_one(f"SELECT COUNT(*) AS n FROM incidents {where}", params)["n"])
        rows = db.query(
            f"SELECT id, rule_id, event_id, metric_name, status, channel_used, attempts, detail, "
            f"created_at, updated_at FROM incidents {where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
            params + [size, (page - 1) * size],
        )
        return {
            "page": page,
            "size": size,
            "total": total,
            "items": [
                {"id": r["id"], "ruleId": r["rule_id"], "eventId": r["event_id"],
                 "metricName": r["metric_name"], "status": r["status"],
                 "channelUsed": r["channel_used"], "attempts": r["attempts"],
                 "detail": r["detail"], "createdAt": r["created_at"], "updatedAt": r["updated_at"]}
                for r in rows
            ],
        }

    @app.get("/internal/v1/incidents/{incident_id}/attempts", tags=["internal"],
             summary="Попытки доставки инцидента (для демонстрации саги)")
    async def incident_attempts(incident_id: str) -> dict:
        rows = db.query(
            "SELECT channel, url, status, error, attempted_at FROM delivery_attempts "
            "WHERE incident_id = ? ORDER BY attempted_at",
            (incident_id,),
        )
        return {
            "incidentId": incident_id,
            "attempts": [
                {"channel": r["channel"], "url": r["url"], "status": r["status"],
                 "error": r["error"], "attemptedAt": r["attempted_at"]}
                for r in rows
            ],
        }

    return app


def main() -> None:
    import uvicorn

    config = Config("notification")
    uvicorn.run(build_app(config), host="0.0.0.0", port=config.port, log_level="info")


if __name__ == "__main__":
    main()
