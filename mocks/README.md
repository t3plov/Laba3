# Моки и заглушки (лабораторная работа №4)

| Компонент из лабы №3 | Чем заменён в реализации | Где |
|---|---|---|
| Keycloak | in-process заглушка в API Gateway: `POST /api/v1/auth/token` выдаёт HS256-JWT с ролями `user`/`admin` (учётные записи `demo/demo`, `admin/admin`) | `services/gateway/main.py` |
| Внешний провайдер уведомлений (email/telegram/webhook) | mock-сервер `channel_mock.py`: контракты `POST /send/{channel}` и `POST /fallback/send/{channel}`, режимы сбоя задаются через `POST /admin/mode`, журнал сообщений — `GET /admin/requests` | `mocks/channel_mock.py` |
| Apache Kafka | в docker-compose — реальный Kafka (KRaft); в локальном single-режиме и в тестах — in-memory шина `shared/events.MemoryBroker` с топиками, группами потребителей и повторной доставкой | `services/shared/events.py` |
| PostgreSQL / TimescaleDB | в docker-compose — реальный PostgreSQL (отдельная база на сервис); локально и в тестах — SQLite-файл на сервис | `services/shared/db.py` |

WireMock не используется: заглушке канала нужно детерминированное управление
режимами сбоя для демонстрации саги (основной канал падает → компенсация
резервным), поэтому реализован собственный mock-сервер с админ-эндпоинтами,
ведущий себя строго по контракту провайдера уведомлений.