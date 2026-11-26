"""Интеграционные тесты сквозных сценариев (сага доставки уведомлений)."""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone

import httpx


def _samples(with_spike: bool):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    samples = []
    for i in range(40):
        value = 50.0 + (i % 4)
        if with_spike and i == 38:
            value = 900.0
        samples.append({"timestamp": (now - timedelta(minutes=40 - i)).isoformat(), "value": value})
    return samples


def _wait_incidents(gateway_url, auth, metric_name, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        response = httpx.get(f"{gateway_url}/api/v1/incidents",
                             params={"size": 50}, headers=auth, timeout=10)
        assert response.status_code == 200
        items = [i for i in response.json()["items"] if i["metricName"] == metric_name]
        done = [i for i in items if i["status"] in ("DELIVERED", "DELIVERY_FAILED")]
        if done:
            return done[0]
        time.sleep(0.3)
    raise AssertionError(f"инцидент для {metric_name} не появился за {timeout} c")


def test_happy_path_end_to_end(servers, gateway_url, auth, channel_admin):
    key = str(uuid.uuid4())
    source = httpx.post(f"{gateway_url}/api/v1/sources",
                        json={"name": f"app-{key[:8]}", "endpoint": "https://app.example/metrics"},
                        headers={**auth, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)
    assert source.status_code == 201, source.text
    source_id = source.json()["id"]

    metric = f"cpu_{key[:8]}"
    metrics = httpx.post(f"{gateway_url}/api/v1/metrics",
                         json={"sourceId": source_id, "metricName": metric, "samples": _samples(True)},
                         headers={**auth, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)
    assert metrics.status_code == 202, metrics.text

    rule = httpx.post(f"{gateway_url}/api/v1/notification-rules",
                      json={"metricName": metric, "condition": "ANOMALY", "channel": "EMAIL"},
                      headers={**auth, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)
    assert rule.status_code == 201, rule.text

    analysis = httpx.post(f"{gateway_url}/api/v1/analyses",
                          json={"sourceId": source_id, "metricName": metric,
                                "from": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
                                "to": datetime.now(timezone.utc).isoformat(),
                                "forecastHorizon": 6},
                          headers={**auth, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)
    assert analysis.status_code == 202, analysis.text

    incident = _wait_incidents(gateway_url, auth, metric)
    assert incident["status"] == "DELIVERED"
    assert incident["channelUsed"] == "EMAIL"

    delivered = httpx.get(f"{channel_admin}/admin/requests", timeout=5).json()["requests"]
    assert any(r["body"]["metricName"] == metric for r in delivered)

    dashboard = httpx.post(f"{gateway_url}/api/v1/dashboards", json={"name": "test board"},
                           headers={**auth, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)
    assert dashboard.status_code == 201
    summary = httpx.get(f"{gateway_url}/api/v1/dashboards/{dashboard.json()['id']}/summary",
                        params={"sourceId": source_id, "metricName": metric,
                                "from": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
                                "to": datetime.now(timezone.utc).isoformat()},
                        headers=auth, timeout=10)
    assert summary.status_code == 200
    body = summary.json()
    assert body["series"]["total"] == 40
    assert body["anomalyFeed"], "лента аномалий заполняется событием anomaly.detected"
    assert body["notificationFeed"], "лента уведомлений заполняется событием notification.delivered"


def test_failure_with_fallback_compensation(servers, gateway_url, auth, channel_admin):
    httpx.post(f"{channel_admin}/admin/mode", json={"primary": "fail", "fallback": "ok"}, timeout=5)
    key = str(uuid.uuid4())
    metric = f"mem_{key[:8]}"
    source = httpx.post(f"{gateway_url}/api/v1/sources",
                        json={"name": f"app-{metric}", "endpoint": "https://app.example/metrics"},
                        headers={**auth, "Idempotency-Key": str(uuid.uuid4())}, timeout=10).json()
    httpx.post(f"{gateway_url}/api/v1/metrics",
               json={"sourceId": source["id"], "metricName": metric, "samples": _samples(True)},
               headers={**auth, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)
    rule = httpx.post(f"{gateway_url}/api/v1/notification-rules",
                      json={"metricName": metric, "condition": "ANOMALY",
                            "channel": "EMAIL", "fallbackChannel": "TELEGRAM"},
                      headers={**auth, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)
    assert rule.status_code == 201
    httpx.post(f"{gateway_url}/api/v1/analyses",
               json={"sourceId": source["id"], "metricName": metric,
                     "from": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
                     "to": datetime.now(timezone.utc).isoformat()},
               headers={**auth, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)

    incident = _wait_incidents(gateway_url, auth, metric, timeout=30)
    assert incident["status"] == "DELIVERED"
    assert incident["channelUsed"] == "TELEGRAM", "компенсация: доставка через резервный канал"
    assert incident["attempts"] == 6, "3 попытки основного канала + 3 попытки резервного"


def test_total_failure_publishes_notification_failed(servers, gateway_url, auth, channel_admin):
    httpx.post(f"{channel_admin}/admin/mode", json={"primary": "fail", "fallback": "fail"}, timeout=5)
    key = str(uuid.uuid4())
    metric = f"err_{key[:8]}"
    source = httpx.post(f"{gateway_url}/api/v1/sources",
                        json={"name": f"app-{metric}", "endpoint": "https://app.example/metrics"},
                        headers={**auth, "Idempotency-Key": str(uuid.uuid4())}, timeout=10).json()
    httpx.post(f"{gateway_url}/api/v1/metrics",
               json={"sourceId": source["id"], "metricName": metric, "samples": _samples(True)},
               headers={**auth, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)
    httpx.post(f"{gateway_url}/api/v1/notification-rules",
               json={"metricName": metric, "condition": "ANOMALY", "channel": "EMAIL"},
               headers={**auth, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)
    httpx.post(f"{gateway_url}/api/v1/analyses",
               json={"sourceId": source["id"], "metricName": metric,
                     "from": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
                     "to": datetime.now(timezone.utc).isoformat()},
               headers={**auth, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)

    incident = _wait_incidents(gateway_url, auth, metric, timeout=30)
    assert incident["status"] == "DELIVERY_FAILED"

    dashboard = httpx.post(f"{gateway_url}/api/v1/dashboards", json={"name": "fail board"},
                           headers={**auth, "Idempotency-Key": str(uuid.uuid4())}, timeout=10).json()
    deadline = time.time() + 10
    feed = []
    while time.time() < deadline:
        summary = httpx.get(f"{gateway_url}/api/v1/dashboards/{dashboard['id']}/summary",
                            params={"sourceId": source["id"], "metricName": metric,
                                    "from": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
                                    "to": datetime.now(timezone.utc).isoformat()},
                            headers=auth, timeout=10).json()
        feed = [n for n in summary["notificationFeed"] if n["status"] == "DELIVERY_FAILED"]
        if feed:
            break
        time.sleep(0.3)
    assert feed, "событие notification.failed дошло до Dashboard Service"