"""Демонстрация сквозных сценариев (шаг 4 лабораторной работы №4).

Запускается против поднятой системы (любой режим: docker compose или
`python scripts/run_local.py --single`):

    python scripts/demo_scenarios.py

Сценарии:
  1. Успешный сквозной путь: приём метрик -> хранение -> анализ -> аномалия ->
     уведомление доставлено (3+ сервиса, синхронные и асинхронные взаимодействия).
  2. Сбой внешнего канала: основной канал падает, компенсирующее действие —
     доставка через резервный канал.
  3. Полный сбой доставки: инцидент DELIVERY_FAILED и событие notification.failed.
"""
from __future__ import annotations

import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import httpx

GATEWAY = "http://127.0.0.1:8080"
NOTIFICATION = "http://127.0.0.1:8084"
CHANNEL_MOCK = "http://127.0.0.1:8089"

RESULTS: list[str] = []


def check(name: str, condition: bool, details: str = "") -> None:
    status = "PASS" if condition else "FAIL"
    line = f"[{status}] {name}" + (f" — {details}" if details else "")
    print(line, flush=True)
    RESULTS.append(line)
    if not condition:
        sys.exit(1)


def samples(with_spike: bool) -> list[dict]:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    out = []
    for i in range(40):
        value = 50.0 + (i % 4)
        if with_spike and i == 38:
            value = 900.0
        out.append({"timestamp": (now - timedelta(minutes=40 - i)).isoformat(), "value": value})
    return out


def wait_incident(headers: dict, metric: str, timeout: float = 40.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        response = httpx.get(f"{GATEWAY}/api/v1/incidents", params={"size": 50}, headers=headers, timeout=10)
        done = [i for i in response.json()["items"]
                if i["metricName"] == metric and i["status"] in ("DELIVERED", "DELIVERY_FAILED")]
        if done:
            return done[0]
        time.sleep(0.5)
    raise AssertionError(f"инцидент для {metric} не завершён за {timeout} c")


def main() -> None:
    token = httpx.post(f"{GATEWAY}/api/v1/auth/token",
                       json={"username": "demo", "password": "demo"}, timeout=10).json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    httpx.post(f"{CHANNEL_MOCK}/admin/reset", timeout=5)
    print("== Токен получен через API Gateway (Keycloak замокан) ==", flush=True)

    # --- Сценарий 1: успешный сквозной путь -------------------------------
    print("\n== Сценарий 1: успешный сквозной путь ==", flush=True)
    key = str(uuid.uuid4())
    metric = f"cpu_{key[:8]}"
    source_key = str(uuid.uuid4())
    source = httpx.post(f"{GATEWAY}/api/v1/sources",
                        json={"name": f"demo-app-{key[:8]}", "endpoint": "https://demo.example/metrics"},
                        headers={**headers, "Idempotency-Key": source_key}, timeout=10)
    check("источник подключён (201)", source.status_code == 201, source.text[:120])
    repeat = httpx.post(f"{GATEWAY}/api/v1/sources",
                        json={"name": f"demo-app-{key[:8]}", "endpoint": "https://demo.example/metrics"},
                        headers={**headers, "Idempotency-Key": source_key}, timeout=10)
    check("идемпотентность Idempotency-Key", repeat.json()["id"] == source.json()["id"])
    source_id = source.json()["id"]

    metrics = httpx.post(f"{GATEWAY}/api/v1/metrics",
                         json={"sourceId": source_id, "metricName": metric, "samples": samples(True)},
                         headers={**headers, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)
    check("метрики приняты (202, Ingestion -> Storage синхронно)", metrics.status_code == 202, metrics.text[:120])

    rule = httpx.post(f"{GATEWAY}/api/v1/notification-rules",
                      json={"metricName": metric, "condition": "ANOMALY",
                            "channel": "EMAIL", "fallbackChannel": "TELEGRAM"},
                      headers={**headers, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)
    check("правило уведомления создано (201)", rule.status_code == 201, rule.text[:120])

    analysis = httpx.post(f"{GATEWAY}/api/v1/analyses",
                          json={"sourceId": source_id, "metricName": metric,
                                "from": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
                                "to": datetime.now(timezone.utc).isoformat(),
                                "forecastHorizon": 6},
                          headers={**headers, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)
    check("анализ запущен (202)", analysis.status_code == 202, analysis.text[:120])

    incident = wait_incident(headers, metric)
    check("аномалия обнаружена и уведомление доставлено", incident["status"] == "DELIVERED",
          f"status={incident['status']} channel={incident['channelUsed']}")

    dashboard = httpx.post(f"{GATEWAY}/api/v1/dashboards", json={"name": "Demo board"},
                           headers={**headers, "Idempotency-Key": str(uuid.uuid4())}, timeout=10).json()
    summary = httpx.get(f"{GATEWAY}/api/v1/dashboards/{dashboard['id']}/summary",
                        params={"sourceId": source_id, "metricName": metric,
                                "from": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
                                "to": datetime.now(timezone.utc).isoformat()},
                        headers=headers, timeout=10).json()
    check("сводка дашборда: ряд из Storage", summary["series"]["total"] == 40,
          f"total={summary['series']['total']}")
    check("сводка дашборда: аномалии из Analysis", len(summary["anomalyFeed"]) >= 1)
    check("сводка дашборда: лента уведомлений по событиям", len(summary["notificationFeed"]) >= 1)

    # --- Сценарий 2: сбой основного канала, компенсация резервным ---------
    print("\n== Сценарий 2: сбой основного канала -> компенсация резервным ==", flush=True)
    httpx.post(f"{CHANNEL_MOCK}/admin/mode", json={"primary": "fail", "fallback": "ok"}, timeout=5)
    metric2 = f"mem_{key[:8]}"
    source2 = httpx.post(f"{GATEWAY}/api/v1/sources",
                         json={"name": f"demo-app-{metric2}", "endpoint": "https://demo.example/metrics"},
                         headers={**headers, "Idempotency-Key": str(uuid.uuid4())}, timeout=10).json()
    httpx.post(f"{GATEWAY}/api/v1/metrics",
               json={"sourceId": source2["id"], "metricName": metric2, "samples": samples(True)},
               headers={**headers, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)
    httpx.post(f"{GATEWAY}/api/v1/notification-rules",
               json={"metricName": metric2, "condition": "ANOMALY",
                     "channel": "EMAIL", "fallbackChannel": "TELEGRAM"},
               headers={**headers, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)
    httpx.post(f"{GATEWAY}/api/v1/analyses",
               json={"sourceId": source2["id"], "metricName": metric2,
                     "from": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
                     "to": datetime.now(timezone.utc).isoformat()},
               headers={**headers, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)
    incident2 = wait_incident(headers, metric2)
    check("компенсирующее действие: доставка через резервный канал",
          incident2["status"] == "DELIVERED" and incident2["channelUsed"] == "TELEGRAM",
          f"status={incident2['status']} channel={incident2['channelUsed']} attempts={incident2['attempts']}")
    attempts = httpx.get(f"{NOTIFICATION}/internal/v1/incidents/{incident2['id']}/attempts", timeout=10).json()
    failed_primary = [a for a in attempts["attempts"] if a["status"] == "FAILED"]
    ok_fallback = [a for a in attempts["attempts"] if a["status"] == "OK"]
    check("повторные попытки основного канала зафиксированы", len(failed_primary) == 3,
          f"failed={len(failed_primary)} ok={len(ok_fallback)}")

    # --- Сценарий 3: полный сбой доставки ---------------------------------
    print("\n== Сценарий 3: оба канала недоступны -> notification.failed ==", flush=True)
    httpx.post(f"{CHANNEL_MOCK}/admin/mode", json={"primary": "fail", "fallback": "fail"}, timeout=5)
    metric3 = f"err_{key[:8]}"
    source3 = httpx.post(f"{GATEWAY}/api/v1/sources",
                         json={"name": f"demo-app-{metric3}", "endpoint": "https://demo.example/metrics"},
                         headers={**headers, "Idempotency-Key": str(uuid.uuid4())}, timeout=10).json()
    httpx.post(f"{GATEWAY}/api/v1/metrics",
               json={"sourceId": source3["id"], "metricName": metric3, "samples": samples(True)},
               headers={**headers, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)
    httpx.post(f"{GATEWAY}/api/v1/notification-rules",
               json={"metricName": metric3, "condition": "ANOMALY", "channel": "EMAIL"},
               headers={**headers, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)
    httpx.post(f"{GATEWAY}/api/v1/analyses",
               json={"sourceId": source3["id"], "metricName": metric3,
                     "from": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
                     "to": datetime.now(timezone.utc).isoformat()},
               headers={**headers, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)
    incident3 = wait_incident(headers, metric3)
    check("инцидент помечен DELIVERY_FAILED", incident3["status"] == "DELIVERY_FAILED",
          f"detail={incident3['detail']}")
    deadline = time.time() + 15
    feed = []
    while time.time() < deadline:
        summary3 = httpx.get(f"{GATEWAY}/api/v1/dashboards/{dashboard['id']}/summary",
                             params={"sourceId": source3["id"], "metricName": metric3,
                                     "from": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
                                     "to": datetime.now(timezone.utc).isoformat()},
                             headers=headers, timeout=10).json()
        feed = [n for n in summary3["notificationFeed"] if n["status"] == "DELIVERY_FAILED"]
        if feed:
            break
        time.sleep(0.5)
    check("событие notification.failed дошло до Dashboard", len(feed) >= 1)

    httpx.post(f"{CHANNEL_MOCK}/admin/reset", timeout=5)
    print("\nВсе сценарии завершены успешно.", flush=True)


if __name__ == "__main__":
    main()