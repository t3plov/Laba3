"""Асинхронное взаимодействие сервисов через брокер сообщений.

События описаны в лабораторной работе №3 (раздел «События»): конверт содержит
eventId, eventType, schemaVersion, occurredAt, correlationId и payload.

Две реализации:

* ``MemoryBroker`` — in-memory шина для локального запуска и тестов без Kafka;
  поддерживает топики, группы потребителей, офсеты и повторную доставку при сбое
  обработчика (имитация redelivery брокера);
* ``KafkaBroker`` — реальный Apache Kafka из docker-compose.

Обработчик, бросивший исключение, приводит к повторной доставке события — поэтому
потребители идемпотентны (дедупликация по eventId).
"""
from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from .config import Config

log = logging.getLogger("events")

Handler = Callable[[dict[str, Any]], None]

TOPICS = (
    "metrics.received",
    "anomaly.detected",
    "forecast.created",
    "notification.delivered",
    "notification.failed",
)


def make_event(event_type: str, payload: dict[str, Any], correlation_id: str | None = None) -> dict[str, Any]:
    return {
        "eventId": str(uuid.uuid4()),
        "eventType": event_type,
        "schemaVersion": 1,
        "occurredAt": datetime.now(timezone.utc).isoformat(),
        "correlationId": correlation_id,
        "payload": payload,
    }


class MemoryBroker:
    """Процесс-локальная шина: топики, группы потребителей, повторная доставка."""

    _registry: dict[str, "MemoryBroker"] = {}
    _registry_lock = threading.Lock()

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._topics: dict[str, list[dict[str, Any]]] = {t: [] for t in TOPICS}
        self._subscriptions: list[tuple[str, str, Handler]] = []
        self._offsets: dict[tuple[str, str], int] = {}
        self._worker = threading.Thread(target=self._pump, daemon=True, name="memory-broker")
        self._stop = threading.Event()
        self._worker.start()

    @classmethod
    def shared(cls) -> "MemoryBroker":
        with cls._registry_lock:
            broker = cls._registry.get("default")
            if broker is None:
                broker = cls()
                cls._registry["default"] = broker
            return broker

    @classmethod
    def reset(cls) -> None:
        with cls._registry_lock:
            broker = cls._registry.pop("default", None)
        if broker:
            broker.close()

    def publish(self, topic: str, event: dict[str, Any]) -> None:
        with self._lock:
            self._topics.setdefault(topic, []).append(event)
        log.info("published %s eventId=%s", topic, event.get("eventId"))

    def subscribe(self, topic: str, group: str, handler: Handler) -> None:
        with self._lock:
            self._subscriptions.append((topic, group, handler))
            self._offsets.setdefault((topic, group), len(self._topics.get(topic, [])))

    def drain(self, timeout: float = 5.0) -> bool:
        """Ждать, пока все опубликованные события не будут обработаны (для тестов)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                pending = any(
                    self._offsets.get((topic, group), 0) < len(self._topics.get(topic, []))
                    for topic, group, _ in self._subscriptions
                )
            if not pending:
                time.sleep(0.05)
                return True
            time.sleep(0.02)
        return False

    def _pump(self) -> None:
        while not self._stop.is_set():
            work = False
            with self._lock:
                subs = list(self._subscriptions)
            for topic, group, handler in subs:
                with self._lock:
                    messages = self._topics.get(topic, [])
                    offset = self._offsets.get((topic, group), 0)
                    if offset >= len(messages):
                        continue
                    event = messages[offset]
                    self._offsets[(topic, group)] = offset + 1
                work = True
                try:
                    handler(event)
                except Exception:  # noqa: BLE001 - имитация redelivery брокера
                    log.exception("handler failed for %s, scheduling redelivery", topic)
                    with self._lock:
                        self._offsets[(topic, group)] = offset
                    time.sleep(0.2)
            if not work:
                time.sleep(0.02)

    def close(self) -> None:
        self._stop.set()
        self._worker.join(timeout=2)


class KafkaBroker:
    """Реальный брокер Apache Kafka (docker-compose)."""

    def __init__(self, config: Config) -> None:
        from kafka import KafkaConsumer, KafkaProducer

        self._bootstrap = config.kafka_bootstrap
        self._producer = KafkaProducer(
            bootstrap_servers=self._bootstrap,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        )
        self._KafkaConsumer = KafkaConsumer
        self._consumers: list[KafkaConsumer] = []

    def publish(self, topic: str, event: dict[str, Any]) -> None:
        key = str(event.get("payload", {}).get("sourceId") or event.get("eventId")).encode("utf-8")
        self._producer.send(topic, value=event, key=key)
        self._producer.flush()
        log.info("published %s eventId=%s", topic, event.get("eventId"))

    def subscribe(self, topic: str, group: str, handler: Handler) -> None:
        consumer = self._KafkaConsumer(
            topic,
            bootstrap_servers=self._bootstrap,
            group_id=group,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        self._consumers.append(consumer)
        thread = threading.Thread(target=self._consume, args=(consumer, handler, topic), daemon=True, name=f"kafka-{topic}")
        thread.start()

    def _consume(self, consumer, handler: Handler, topic: str) -> None:
        for message in consumer:
            try:
                handler(message.value)
            except Exception:  # noqa: BLE001 - событие будет передано повторно после рестарта/ребаланса
                log.exception("handler failed for %s", topic)

    def close(self) -> None:
        self._producer.close()
        for consumer in self._consumers:
            consumer.close()


def create_broker(config: Config):
    if config.broker_backend == "kafka":
        return KafkaBroker(config)
    return MemoryBroker.shared()
