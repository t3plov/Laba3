"""Единая модель ошибок во всех сервисах: Problem Details, RFC 7807.

Схема `Problem` описана в лабораторной работе №3 (api/*.yaml, components.schemas.Problem).
"""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

PROBLEM_JSON = "application/problem+json"


class Problem(Exception):
    def __init__(
        self,
        status: int,
        title: str,
        detail: str,
        type_uri: str = "https://example.com/errors/problem",
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(detail)
        self.status = status
        self.title = title
        self.detail = detail
        self.type_uri = type_uri
        self.correlation_id = correlation_id

    def to_body(self) -> dict:
        body = {
            "type": self.type_uri,
            "title": self.title,
            "status": self.status,
            "detail": self.detail,
        }
        if self.correlation_id:
            body["correlationId"] = self.correlation_id
        return body


def problem_response(status: int, title: str, detail: str, type_uri: str, correlation_id: str | None) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        media_type=PROBLEM_JSON,
        content=Problem(status, title, detail, type_uri, correlation_id).to_body(),
    )


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(Problem)
    async def _problem(request: Request, exc: Problem) -> JSONResponse:
        cid = request.headers.get("X-Correlation-Id") or exc.correlation_id
        return problem_response(exc.status, exc.title, exc.detail, exc.type_uri, cid)

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError) -> JSONResponse:
        cid = request.headers.get("X-Correlation-Id")
        errors = "; ".join(".".join(str(p) for p in e["loc"][1:]) + f": {e['msg']}" for e in exc.errors())
        return problem_response(400, "Invalid request", errors or "Некорректные параметры", "https://example.com/errors/validation", cid)
