"""Минимальная реализация JWT (HS256) для мока авторизации.

В лабораторной работе №3 авторизация вынесена в Keycloak, поэтому собственный сервис
пользователей не пишется. Здесь Keycloak заменён заглушкой: токен выдаёт mock-сервер,
а сервисы проверяют подпись тем же секретом разработки.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _unb64(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def create_token(subject: str, roles: list[str], secret: str, ttl: int = 3600) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    now = int(time.time())
    claims = {
        "sub": subject,
        "preferred_username": subject,
        "roles": roles,
        "iat": now,
        "exp": now + ttl,
    }
    signing_input = f"{_b64(json.dumps(header).encode())}.{_b64(json.dumps(claims).encode())}"
    signature = hmac.new(secret.encode(), signing_input.encode(), hashlib.sha256).digest()
    return f"{signing_input}.{_b64(signature)}"


def verify_token(token: str, secret: str) -> dict[str, Any] | None:
    try:
        header_b64, payload_b64, signature_b64 = token.split(".")
        signing_input = f"{header_b64}.{payload_b64}"
        expected = hmac.new(secret.encode(), signing_input.encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(expected, _unb64(signature_b64)):
            return None
        claims = json.loads(_unb64(payload_b64))
        if int(claims.get("exp", 0)) < int(time.time()):
            return None
        return claims
    except Exception:  # noqa: BLE001 - любой некорректный токен считаем недействительным
        return None
