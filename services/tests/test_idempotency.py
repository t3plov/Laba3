"""Unit-тест идемпотентности и интеграционный тест повторного запроса."""
from __future__ import annotations

import uuid


def test_idempotent_source_creation(servers, gateway_url, auth):
    import httpx

    key = str(uuid.uuid4())
    payload = {"name": f"src-{key[:8]}", "endpoint": "https://example.com/metrics"}
    first = httpx.post(f"{gateway_url}/api/v1/sources", json=payload,
                       headers={**auth, "Idempotency-Key": key}, timeout=10)
    assert first.status_code == 201, first.text
    second = httpx.post(f"{gateway_url}/api/v1/sources", json=payload,
                        headers={**auth, "Idempotency-Key": key}, timeout=10)
    assert second.status_code == 201, second.text
    assert first.json()["id"] == second.json()["id"], "повторный запрос с тем же ключом возвращает прежний результат"

    other = httpx.post(f"{gateway_url}/api/v1/sources", json=payload,
                       headers={**auth, "Idempotency-Key": str(uuid.uuid4())}, timeout=10)
    assert other.status_code == 409
    assert other.headers["content-type"] == "application/problem+json"
    assert other.json()["status"] == 409


def test_missing_idempotency_key_rejected(servers, gateway_url, auth):
    import httpx

    response = httpx.post(f"{gateway_url}/api/v1/sources",
                          json={"name": "no-key", "endpoint": "https://example.com"},
                          headers=auth, timeout=10)
    assert response.status_code == 400
    assert response.headers["content-type"] == "application/problem+json"


def test_unauthorized_without_token(servers, gateway_url):
    import httpx

    response = httpx.get(f"{gateway_url}/api/v1/sources", timeout=10)
    assert response.status_code == 401