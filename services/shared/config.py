"""Конфигурация сервиса: порты, хранилище, брокер, адреса соседей."""
from __future__ import annotations

import os

# Порты соответствуют схеме из лабораторной работы №3.
PORTS = {
    "gateway": 8080,
    "ingestion": 8081,
    "storage": 8082,
    "analysis": 8083,
    "notification": 8084,
    "dashboard": 8085,
}


def env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


class Config:
    def __init__(self, service: str) -> None:
        self.service = service
        self.port = int(env(f"{service.upper()}_PORT", str(PORTS[service])))

        # Хранилище: реальный PostgreSQL из docker-compose или SQLite для локального запуска.
        self.db_backend = env("DB_BACKEND", "sqlite")
        self.database_url = env("DATABASE_URL", "")
        self.sqlite_path = env(
            "SQLITE_PATH",
            os.path.join(env("DATA_DIR", os.path.join(os.getcwd(), "data")), f"{service}.db"),
        )

        # Брокер: Kafka из docker-compose или in-memory шина для тестов и запуска без Docker.
        self.broker_backend = env("BROKER_BACKEND", "memory")
        self.kafka_bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9094")

        # Адреса соседних сервисов. В docker-compose перекрываются именами контейнеров.
        self.storage_url = env("STORAGE_URL", f"http://127.0.0.1:{PORTS['storage']}")
        self.analysis_url = env("ANALYSIS_URL", f"http://127.0.0.1:{PORTS['analysis']}")
        self.gateway_url = env("GATEWAY_URL", f"http://127.0.0.1:{PORTS['gateway']}")

        # Внешний канал доставки уведомлений — заглушка WireMock.
        self.channel_url = env("NOTIFICATION_CHANNEL_URL", "http://127.0.0.1:8089")
        self.fallback_channel_url = env("NOTIFICATION_FALLBACK_URL", "http://127.0.0.1:8089/fallback")

        # Мок Keycloak: токен выдаётся stub-сервером, проверка — HMAC-ключом разработки.
        self.jwt_secret = env("JWT_SECRET", "dev-secret-key")
        self.token_url = env("KEYCLOAK_TOKEN_URL", "http://127.0.0.1:8089/realms/master/protocol/openid-connect/token")

        self.artifact_dir = env("ARTIFACT_DIR", os.path.join(env("DATA_DIR", os.path.join(os.getcwd(), "data")), "models"))

    def port_of(self, service: str) -> int:
        return int(env(f"{service.upper()}_PORT", str(PORTS[service])))

    def internal_url(self, service: str) -> str:
        base = {
            "storage": self.storage_url,
            "analysis": self.analysis_url,
        }.get(service)
        if base is None:
            base = f"http://127.0.0.1:{self.port_of(service)}"
        return base.rstrip("/")


def ensure_dirs(config: Config) -> None:
    os.makedirs(os.path.dirname(config.sqlite_path) or ".", exist_ok=True)
    os.makedirs(config.artifact_dir, exist_ok=True)
