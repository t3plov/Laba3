"""Локальный запуск всей системы без Docker.

Поднимает заглушку канала уведомлений и все шесть сервисов отдельными процессами.
Хранилища — SQLite (своя база на сервис), брокер — in-memory шина внутри каждого
процесса не подойдёт для межпроцессного обмена, поэтому в локальном режиме
используется общий in-memory брокер... но процессы изолированы!

ВАЖНО: in-memory брокер живёт внутри процесса, поэтому в локальном режиме без
Kafka асинхронные события не пересекут границу процесса. Для полноценной
демонстрации асинхронных взаимодействий локально поднимается Kafka из
docker-compose (BROKER_BACKEND=kafka), либо используется режим `--single`,
в котором все сервисы работают в одном процессе (in-memory брокер общий).

Режимы:
    python scripts/run_local.py            # процессы + Kafka из docker compose
    python scripts/run_local.py --single   # один процесс, in-memory брокер, SQLite
"""
from __future__ import annotations

import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVICES_DIR = os.path.join(ROOT, "services")

SERVICES = ["gateway", "ingestion", "storage", "analysis", "notification", "dashboard"]
MODULES = {
    "gateway": "gateway.main",
    "ingestion": "metrics_ingestion.main",
    "storage": "metric_storage.main",
    "analysis": "analysis.main",
    "notification": "notification.main",
    "dashboard": "dashboard.main",
}


def run_single() -> None:
    """Все сервисы в одном процессе: общий in-memory брокер и SQLite-файлы."""
    import threading

    import uvicorn

    sys.path.insert(0, SERVICES_DIR)
    os.environ.setdefault("BROKER_BACKEND", "memory")
    os.environ.setdefault("DB_BACKEND", "sqlite")
    os.environ.setdefault("DATA_DIR", os.path.join(ROOT, "data"))

    from analysis.main import build_app as analysis_app
    from dashboard.main import build_app as dashboard_app
    from gateway.main import build_app as gateway_app
    from metric_storage.main import build_app as storage_app
    from metrics_ingestion.main import build_app as ingestion_app
    from notification.main import build_app as notification_app
    from shared.config import Config

    apps = {
        "gateway": (gateway_app(Config("gateway")), 8080),
        "ingestion": (ingestion_app(Config("ingestion")), 8081),
        "storage": (storage_app(Config("storage")), 8082),
        "analysis": (analysis_app(Config("analysis")), 8083),
        "notification": (notification_app(Config("notification")), 8084),
        "dashboard": (dashboard_app(Config("dashboard")), 8085),
    }
    servers = []
    for name, (app, port) in apps.items():
        config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
        server = uvicorn.Server(config)
        thread = threading.Thread(target=server.run, daemon=True, name=name)
        thread.start()
        servers.append(server)

    # Заглушка канала уведомлений в том же процессе.
    sys.path.insert(0, ROOT)
    from mocks.channel_mock import app as channel_app

    channel_config = uvicorn.Config(channel_app, host="127.0.0.1", port=8089, log_level="warning")
    channel_server = uvicorn.Server(channel_config)
    threading.Thread(target=channel_server.run, daemon=True, name="channel-mock").start()
    servers.append(channel_server)

    print("Сервисы подняты: gateway=8080 ingestion=8081 storage=8082 analysis=8083 "
          "notification=8084 dashboard=8085 channel-mock=8089")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for server in servers:
            server.should_exit = True


def run_processes() -> None:
    env = dict(os.environ)
    env.setdefault("BROKER_BACKEND", "kafka")
    env.setdefault("DB_BACKEND", "sqlite")
    env.setdefault("DATA_DIR", os.path.join(ROOT, "data"))
    env["PYTHONPATH"] = SERVICES_DIR
    procs = [subprocess.Popen([sys.executable, "-m", "mocks.channel_mock"], cwd=ROOT, env=env)]
    for name in SERVICES:
        procs.append(subprocess.Popen([sys.executable, "-m", MODULES[name]], cwd=SERVICES_DIR, env=env))
    print("Сервисы подняты (брокер Kafka из docker-compose). Ctrl+C для остановки.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        for proc in procs:
            proc.terminate()


if __name__ == "__main__":
    if "--single" in sys.argv:
        run_single()
    else:
        run_processes()