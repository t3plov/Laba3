"""Фабрика приложения сервиса: логирование, correlation id, JWT-проверка, ошибки.

Внешние (public) эндпоинты доступны только через API Gateway; Gateway передаёт
проверенный токен. Внутренние (internal) эндпоинты помечены тегом `internal` и в
демонстрации вызываются напрямую между сервисами.
"""
from __future__ import annotations

import logging
import time
import uuid
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .config import Config
from .errors import Problem, install_error_handlers
from .jwt_util import verify_token

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("app")

PUBLIC_PREFIXES = ("/api/",)


def create_app(config: Config, title: str) -> FastAPI:
    app = FastAPI(title=title, version="1.0.0")
    app.state.config = config
    install_error_handlers(app)

    @app.middleware("http")
    async def correlation_and_auth(request: Request, call_next):
        correlation_id = request.headers.get("X-Correlation-Id") or str(uuid.uuid4())
        request.state.correlation_id = correlation_id
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:  # noqa: BLE001
            log.exception("unhandled error on %s %s", request.method, request.url.path)
            response = JSONResponse(
                status_code=500,
                media_type="application/problem+json",
                content=Problem(500, "Internal error", "Внутренняя ошибка сервиса",
                                  "https://example.com/errors/internal", correlation_id).to_body(),
            )
        elapsed = (time.perf_counter() - started) * 1000
        log.info("%s %s -> %s in %.1f ms cid=%s", request.method, request.url.path,
                 response.status_code, elapsed, correlation_id)
        response.headers["X-Correlation-Id"] = correlation_id
        return response

    @app.middleware("http")
    async def authenticate(request: Request, call_next):
        path = request.url.path
        needs_auth = path.startswith(PUBLIC_PREFIXES) and path != "/api/v1/health"
        if not needs_auth:
            return await call_next(request)
        header = request.headers.get("Authorization", "")
        token = header[7:] if header.startswith("Bearer ") else None
        claims: dict[str, Any] | None = verify_token(token, config.jwt_secret) if token else None
        if claims is None:
            correlation_id = request.headers.get("X-Correlation-Id")
            return JSONResponse(
                status_code=401,
                media_type="application/problem+json",
                content=Problem(401, "Unauthorized", "Отсутствует или недействителен токен",
                                  "https://example.com/errors/unauthorized", correlation_id).to_body(),
            )
        if path.startswith("/api/v1/admin") and "admin" not in claims.get("roles", []):
            correlation_id = request.headers.get("X-Correlation-Id")
            return JSONResponse(
                status_code=403,
                media_type="application/problem+json",
                content=Problem(403, "Forbidden", "Недостаточно прав",
                                  "https://example.com/errors/forbidden", correlation_id).to_body(),
            )
        request.state.user = claims
        return await call_next(request)

    @app.get("/healthz", tags=["internal"])
    async def healthz() -> dict[str, str]:
        return {"status": "ok", "service": config.service}

    return app
