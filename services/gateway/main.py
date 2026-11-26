"""API Gateway (порт 8080).

Единственная точка входа для клиента (требование лабораторной работы №3).
Маршрутизирует запросы к бизнес-сервисам, проксирует заголовки Authorization,
Idempotency-Key и X-Correlation-Id. Keycloak заменён in-process заглушкой:
эндпоинт /api/v1/auth/token выдаёт JWT с ролями user/admin (секрет разработки).
"""
from __future__ import annotations

import logging

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import Response
from pydantic import BaseModel

from shared.config import Config
from shared.errors import Problem, install_error_handlers
from shared.jwt_util import create_token

log = logging.getLogger("gateway")

ROUTES: list[tuple[str, str]] = [
    ("/api/v1/sources", "ingestion"),
    ("/api/v1/metrics", "ingestion"),
    ("/api/v1/series", "storage"),
    ("/api/v1/analyses", "analysis"),
    ("/api/v1/notification-rules", "notification"),
    ("/api/v1/incidents", "notification"),
    ("/api/v1/dashboards", "dashboard"),
]

FORWARDED_HEADERS = ("authorization", "idempotency-key", "x-correlation-id")

USERS = {
    "demo": ("demo", ["user"]),
    "admin": ("admin", ["user", "admin"]),
}


class TokenRequest(BaseModel):
    username: str
    password: str


def build_app(config: Config) -> FastAPI:
    app = FastAPI(title="api-gateway", version="1.0.0")
    install_error_handlers(app)

    @app.post("/api/v1/auth/token", tags=["public"], summary="Выдать JWT (заглушка Keycloak)")
    async def token(payload: TokenRequest) -> dict:
        user = USERS.get(payload.username)
        if user is None or user[0] != payload.password:
            raise Problem(401, "Unauthorized", "Неверные учётные данные",
                          "https://example.com/errors/unauthorized")
        return {"access_token": create_token(payload.username, user[1], config.jwt_secret),
                "token_type": "Bearer", "expires_in": 3600}

    @app.get("/healthz", tags=["internal"])
    async def healthz() -> dict:
        return {"status": "ok", "service": "gateway"}

    @app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
    async def proxy(request: Request, path: str) -> Response:
        full_path = f"/api/{path}"
        target = next((service for prefix, service in ROUTES if full_path.startswith(prefix)), None)
        if target is None:
            raise Problem(404, "Not found", f"Маршрут {full_path} не настроен в Gateway",
                          "https://example.com/errors/no-route")
        url = f"{config.internal_url(target)}{full_path}"
        headers = {name: value for name, value in request.headers.items() if name.lower() in FORWARDED_HEADERS}
        body = await request.body()
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                upstream = await client.request(
                    request.method,
                    url,
                    params=request.query_params,
                    content=body,
                    headers=headers,
                )
        except httpx.HTTPError as exc:
            raise Problem(503, "Service unavailable", f"Сервис {target} недоступен: {exc}",
                          "https://example.com/errors/upstream-unavailable") from exc
        log.info("proxy %s %s -> %s (%s)", request.method, full_path, upstream.status_code, target)
        return Response(
            content=upstream.content,
            status_code=upstream.status_code,
            media_type=upstream.headers.get("content-type"),
            headers={"X-Correlation-Id": upstream.headers.get("X-Correlation-Id", "")},
        )

    return app


def main() -> None:
    import uvicorn

    config = Config("gateway")
    uvicorn.run(build_app(config), host="0.0.0.0", port=config.port, log_level="info")


if __name__ == "__main__":
    main()