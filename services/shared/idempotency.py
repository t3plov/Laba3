"""Идемпотентность операций, которые могут повторяться.

Согласно контрактам лабораторной работы №3, повторяемые операции принимают заголовок
`Idempotency-Key`. Сервис запоминает ключ и результат первой обработки: повторный запрос
с тем же ключом возвращает прежний ответ, а не создаёт вторую сущность.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .db import Database, dumps, loads

SCHEMA = """
CREATE TABLE IF NOT EXISTS idempotency (
    key TEXT PRIMARY KEY,
    status INTEGER NOT NULL,
    body TEXT NOT NULL,
    created_at TEXT NOT NULL
)
"""


class IdempotencyStore:
    def __init__(self, db: Database) -> None:
        self._db = db
        self._db._run_script(SCHEMA)

    def get(self, key: str) -> dict[str, Any] | None:
        row = self._db.query_one("SELECT status, body FROM idempotency WHERE key = ?", (key,))
        if row is None:
            return None
        return {"status": int(row["status"]), "body": loads(row["body"])}

    def remember(self, key: str, status: int, body: dict[str, Any]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self._db.execute(
            "INSERT OR IGNORE INTO idempotency (key, status, body, created_at) VALUES (?, ?, ?, ?)",
            (key, status, dumps(body), now),
        )
