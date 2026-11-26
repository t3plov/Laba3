"""Хранилище данных сервиса.

Каждый сервис обращается только к своей базе — общая база между сервисами запрещена
требованием лабораторной работы №3. Поддерживаются два режима:

* PostgreSQL (реальный, из docker-compose) — отдельная база на каждый сервис;
* SQLite — локальный запуск и интеграционные тесты без внешних зависимостей.

SQL пишется с плейсхолдерами `?`; для PostgreSQL они переводятся в `%s`.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Any

from .config import Config


class Database:
    def __init__(self, config: Config, schema: str) -> None:
        self.backend = config.db_backend
        self._lock = threading.RLock()
        if self.backend == "postgres":
            import psycopg

            dsn = config.database_url or os.environ["DATABASE_URL"]
            self._conn = psycopg.connect(dsn, autocommit=True)
        else:
            os.makedirs(os.path.dirname(config.sqlite_path) or ".", exist_ok=True)
            self._conn = sqlite3.connect(config.sqlite_path, check_same_thread=False, isolation_level=None)
            self._conn.row_factory = sqlite3.Row
        self._run_script(schema)

    def _run_script(self, schema: str) -> None:
        with self._lock:
            for statement in [s.strip() for s in schema.split(";") if s.strip()]:
                self._conn.execute(statement)

    def _adapt(self, sql: str) -> str:
        return sql.replace("?", "%s") if self.backend == "postgres" else sql

    def execute(self, sql: str, params: tuple | list = ()) -> None:
        with self._lock:
            self._conn.execute(self._adapt(sql), tuple(params))

    def executemany(self, sql: str, rows: list[tuple]) -> None:
        if not rows:
            return
        with self._lock:
            self._conn.executemany(self._adapt(sql), [tuple(r) for r in rows])

    def query(self, sql: str, params: tuple | list = ()) -> list[dict[str, Any]]:
        with self._lock:
            cursor = self._conn.execute(self._adapt(sql), tuple(params))
            return [dict(row) for row in cursor.fetchall()]

    def query_one(self, sql: str, params: tuple | list = ()) -> dict[str, Any] | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def close(self) -> None:
        with self._lock:
            self._conn.close()


def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def loads(value: str | None) -> Any:
    return json.loads(value) if value else None
