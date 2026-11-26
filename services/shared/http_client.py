"""Синхронные REST-вызовы между сервисами согласно контрактам лабы №3.

Все ошибки соседей преобразуются в единую модель Problem, чтобы вызывающий сервис
мог единообразно реагировать на сбои (требование лабораторной работы №4).
"""
from __future__ import annotations

import logging
from typing import Any

import httpx

from .errors import Problem

log = logging.getLogger("http_client")

TIMEOUT = 5.0


class ServiceCallError(Problem):
    """Соседний сервис ответил ошибкой или недоступен."""


def _headers(correlation_id: str | None) -> dict[str, str]:
    headers = {"Accept": "application/json"}
    if correlation_id:
        headers["X-Correlation-Id"] = correlation_id
    return headers


def _raise_for_status(response: httpx.Response, correlation_id: str | None) -> None:
    if response.status_code < 400:
        return
    try:
        body = response.json()
    except Exception:  # noqa: BLE001
        body = {}
    raise ServiceCallError(
        status=response.status_code,
        title=str(body.get("title", "Upstream error")),
        detail=str(body.get("detail", response.text[:200])),
        type_uri=str(body.get("type", "https://example.com/errors/upstream")),
        correlation_id=correlation_id,
    )


def get_json(url: str, params: dict[str, Any] | None = None, correlation_id: str | None = None) -> Any:
    log.info("GET %s params=%s", url, params)
    with httpx.Client(timeout=TIMEOUT) as client:
        response = client.get(url, params=params, headers=_headers(correlation_id))
    _raise_for_status(response, correlation_id)
    return response.json()


def post_json(url: str, body: dict[str, Any] | None = None, correlation_id: str | None = None,
              headers: dict[str, str] | None = None) -> tuple[int, Any]:
    log.info("POST %s", url)
    merged = _headers(correlation_id)
    merged.update(headers or {})
    with httpx.Client(timeout=TIMEOUT) as client:
        response = client.post(url, json=body, headers=merged)
    _raise_for_status(response, correlation_id)
    return response.status_code, response.json()
