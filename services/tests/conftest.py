"""Общие фикстуры интеграционных тестов.

Все сервисы поднимаются в одном процессе на тестовых портах: in-memory брокер
общий, хранилища — временные SQLite-файлы, канал уведомлений — заглушка.
"""
from __future__ import annotations

import os
import sys
import threading
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVICES_DIR = os.path.join(ROOT, "services")
sys.path.insert(0, SERVICES_DIR)
sys.path.insert(0, ROOT)

PORTS = {
    "gateway": 18080,
    "ingestion": 18081,
    "storage": 18082,
    "analysis": 18083,
    "notification": 18084,
    "dashboard": 18085,
    "channel": 18089,
}


@pytest.fixture(scope="session")
def env_setup(tmp_path_factory):
    data_dir = tmp_path_factory.mktemp("data")
    os.environ["DATA_DIR"] = str(data_dir)
    os.environ["DB_BACKEND"] = "sqlite"
    os.environ["BROKER_BACKEND"] = "memory"
    os.environ["JWT_SECRET"] = "test-secret"
    for name in PORTS:
        if name == "channel":
            continue
        os.environ[f"{name.upper()}_PORT"] = str(PORTS[name])
    os.environ["STORAGE_URL"] = f"http://127.0.0.1:{PORTS['storage']}"
    os.environ["ANALYSIS_URL"] = f"http://127.0.0.1:{PORTS['analysis']}"
    os.environ["NOTIFICATION_CHANNEL_URL"] = f"http://127.0.0.1:{PORTS['channel']}"
    os.environ["NOTIFICATION_FALLBACK_URL"] = f"http://127.0.0.1:{PORTS['channel']}/fallback"
    return os.environ


@pytest.fixture(scope="session")
def servers(env_setup):
    import uvicorn

    from analysis.main import build_app as analysis_app
    from dashboard.main import build_app as dashboard_app
    from gateway.main import build_app as gateway_app
    from metric_storage.main import build_app as storage_app
    from metrics_ingestion.main import build_app as ingestion_app
    from mocks.channel_mock import app as channel_app
    from notification.main import build_app as notification_app
    from shared.config import Config

    apps = [
        ("gateway", gateway_app(Config("gateway")), PORTS["gateway"]),
        ("ingestion", ingestion_app(Config("ingestion")), PORTS["ingestion"]),
        ("storage", storage_app(Config("storage")), PORTS["storage"]),
        ("analysis", analysis_app(Config("analysis")), PORTS["analysis"]),
        ("notification", notification_app(Config("notification")), PORTS["notification"]),
        ("dashboard", dashboard_app(Config("dashboard")), PORTS["dashboard"]),
        ("channel", channel_app, PORTS["channel"]),
    ]
    servers = []
    for name, app, port in apps:
        server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
        thread = threading.Thread(target=server.run, daemon=True, name=f"srv-{name}")
        thread.start()
        servers.append(server)

    import httpx

    healthy = False
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            if httpx.get(f"http://127.0.0.1:{PORTS['gateway']}/healthz", timeout=1).status_code == 200:
                healthy = True
                break
        except Exception:
            time.sleep(0.2)
    alive = {t.name: t.is_alive() for t in threading.enumerate() if t.name.startswith("srv-")}
    print(f"[conftest] healthy={healthy} threads={alive}", flush=True)
    assert healthy, f"сервисы не поднялись: {alive}"
    yield servers
    for server in servers:
        server.should_exit = True


@pytest.fixture(scope="session")
def gateway_url(env_setup) -> str:
    return f"http://127.0.0.1:{PORTS['gateway']}"


@pytest.fixture(scope="session")
def token(servers, gateway_url) -> str:
    import httpx

    response = httpx.post(f"{gateway_url}/api/v1/auth/token",
                          json={"username": "demo", "password": "demo"}, timeout=5)
    assert response.status_code == 200, response.text
    return response.json()["access_token"]


@pytest.fixture()
def auth(token) -> dict:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture()
def channel_admin(env_setup):
    import httpx

    base = f"http://127.0.0.1:{PORTS['channel']}"
    httpx.post(f"{base}/admin/reset", timeout=5)
    return base