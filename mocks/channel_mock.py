"""Заглушка внешнего канала доставки уведомлений (email/webhook/telegram).

Заменяет внешнего провайдера уведомлений. Ведёт себя согласно контракту:
POST /send/{channel} и POST /fallback/send/{channel} возвращают 200 или 500
в зависимости от режима, заданного через админ-эндпоинт:

    POST /admin/mode {"primary": "ok"|"fail", "fallback": "ok"|"fail"}

Все полученные сообщения доступны для проверки: GET /admin/requests.
"""
from __future__ import annotations

import threading

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI(title="notification-channel-mock")
_lock = threading.Lock()
_mode = {"primary": "ok", "fallback": "ok"}
_requests: list[dict] = []


@app.post("/admin/mode")
async def set_mode(payload: dict) -> dict:
    with _lock:
        for key in ("primary", "fallback"):
            if key in payload:
                _mode[key] = payload[key]
    return _mode


@app.get("/admin/mode")
async def get_mode() -> dict:
    with _lock:
        return dict(_mode)


@app.get("/admin/requests")
async def requests_log() -> dict:
    with _lock:
        return {"requests": list(_requests)}


@app.post("/admin/reset")
async def reset() -> dict:
    with _lock:
        _requests.clear()
        _mode.update({"primary": "ok", "fallback": "ok"})
    return {"status": "reset"}


def _handle(scope: str, channel: str):
    with _lock:
        mode = _mode[scope]
    if mode == "fail":
        return JSONResponse(status_code=500, content={"error": "channel unavailable"})
    return JSONResponse(status_code=200, content={"status": "accepted", "channel": channel})


@app.post("/send/{channel}")
async def send(channel: str, request: Request) -> JSONResponse:
    body = await request.json()
    with _lock:
        _requests.append({"scope": "primary", "channel": channel, "body": body})
    return _handle("primary", channel)


@app.post("/fallback/send/{channel}")
async def fallback_send(channel: str, request: Request) -> JSONResponse:
    body = await request.json()
    with _lock:
        _requests.append({"scope": "fallback", "channel": channel, "body": body})
    return _handle("fallback", channel)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8089, log_level="warning")