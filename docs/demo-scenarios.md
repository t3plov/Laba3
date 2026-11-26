# Демонстрационные сценарии (лабораторная работа №4)

Проверены командой `python scripts/demo_scenarios.py` против системы, поднятой в
режиме `python scripts/run_local.py --single`. Полный вывод: [demo-output.txt](demo-output.txt).

## Какие взаимодействия реальные, а какие через моки

| Взаимодействие | Реализация |
|---|---|
| Клиент → API Gateway → сервисы (REST) | реально (HTTP-проксирование в Gateway) |
| Ingestion → Storage `POST /internal/v1/series` (REST) | реально |
| Analysis → Storage `GET /internal/v1/series` (REST) | реально |
| Dashboard → Storage / Analysis (REST) | реально |
| Ingestion → Analysis `metrics.received` (асинхронно) | in-memory брокер (в docker-compose — реальный Kafka) |
| Analysis → Notification/Dashboard `anomaly.detected`, `forecast.created` | in-memory брокер / Kafka |
| Notification → Dashboard `notification.delivered` / `notification.failed` | in-memory брокер / Kafka |
| Notification → внешний канал уведомлений | мок `mocks/channel_mock.py` |
| Авторизация (Keycloak) | мок: JWT выдаёт API Gateway (`/api/v1/auth/token`) |
| Хранилища | SQLite-файл на сервис (в docker-compose — PostgreSQL, отдельная база на сервис) |

## Сценарий 1. Успешный сквозной путь (happy path)

Участвуют: Gateway, Ingestion, Storage, Analysis, Notification, Dashboard и заглушка канала.

1. `POST /api/v1/auth/token` — токен (demo/demo).
2. `POST /api/v1/sources` — подключение источника; повтор с тем же `Idempotency-Key`
   возвращает прежний `id` (идемпотентность).
3. `POST /api/v1/metrics` — пакет из 40 точек с аномальным всплеском; Ingestion
   синхронно пишет ряд в Storage и публикует `metrics.received`.
4. `POST /api/v1/notification-rules` — правило: аномалия метрики → EMAIL (резерв TELEGRAM).
5. `POST /api/v1/analyses` — Analysis синхронно читает ряд из Storage, находит аномалию
   (rolling z-score), строит прогноз и публикует `anomaly.detected` + `forecast.created`.
6. Notification создаёт инцидент и доставляет уведомление в канал → `notification.delivered`.
7. `GET /api/v1/dashboards/{id}/summary` — Dashboard синхронно тянет ряд и результаты
   анализа, лента заполнена событиями.

Фрагмент вывода:

```text
[PASS] источник подключён (201)
[PASS] идемпотентность Idempotency-Key
[PASS] метрики приняты (202, Ingestion -> Storage синхронно)
[PASS] аномалия обнаружена и уведомление доставлено — status=DELIVERED channel=EMAIL
[PASS] сводка дашборда: ряд из Storage — total=40
```

## Сценарий 2. Сбой внешнего канала и компенсирующее действие

Режим заглушки: `POST :8089/admin/mode {"primary": "fail", "fallback": "ok"}`.

1. Notification получает `anomaly.detected`, создаёт инцидент `PENDING`.
2. Три попытки доставки в основной канал (EMAIL) завершаются HTTP 500 — попытки
   фиксируются в таблице delivery_attempts.
3. Компенсирующее действие: переключение на резервный канал (TELEGRAM) — доставка успешна.
4. Инцидент `DELIVERED`, опубликовано `notification.delivered`.

```text
[PASS] компенсирующее действие: доставка через резервный канал — status=DELIVERED channel=TELEGRAM attempts=6
[PASS] повторные попытки основного канала зафиксированы — failed=3 ok=1
```

## Сценарий 3. Полный сбой доставки

Режим заглушки: оба канала отвечают 500.

1. Исчерпаны попытки основного и резервного каналов (резерв не настроен).
2. Инцидент переведён в `DELIVERY_FAILED`.
3. Опубликовано событие `notification.failed`; Dashboard Service получил его и
   отразил в ленте уведомлений сводки.

```text
[PASS] инцидент помечен DELIVERY_FAILED — detail=HTTP 500
[PASS] событие notification.failed дошло до Dashboard
```

## Повторная доставка события (идемпотентность потребителя)

Потребители Notification и Dashboard хранят обработанные `eventId` (таблица
processed_events) и игнорируют повторную доставку того же события брокером —
поведение проверено интеграционными тестами `services/tests`.