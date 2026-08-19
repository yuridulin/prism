# Prism

Стенд сравнения time-series стеков под задачу **каталога тегов** и append-only рядов.

Запись: `ts` (UTC), `tag_id` (uint32), `value` (float), `quality` (OPC DA uint16, 192 = Good).

Чтение:

| Режим | Смысл |
| --- | --- |
| `locf` | last observation carried forward на момент `at` |
| `range` | locf на `from` + все точки `(from, to]` |
| `sample` | range, затем сетка с протяжкой LOCF |
| `twavg` | range, затем средневзвешенное по времени |

Первые два — основные. `sample` и `twavg` считаются в API-слое из `range`, чтобы сравнивать хранилища честно.

| Слой | Варианты |
| --- | --- |
| API | Go `:8081`, Python `:8082`, C# `:8083`, Rust `:8084` |
| Storage | QuestDB, ClickHouse, TimescaleDB, InfluxDB 2, VictoriaMetrics |
| Шина | NATS JetStream (`prism.samples`) |
| Наблюдение | Prometheus + Grafana |
| Нагрузка | YAML-профили → генератор или k6 |

Хранилище: `PRISM_STORAGE` / `GO_API_STORAGE` / `PYTHON_API_STORAGE` / `CSHARP_API_STORAGE` / `RUST_API_STORAGE`.

## Быстрый старт

```powershell
copy .env.example .env
docker compose up -d --build go-api questdb nats prometheus
```

```powershell
curl http://localhost:8081/v1/meta
curl -X POST http://localhost:8081/v1/write -H "Content-Type: application/json" -d "{\"samples\":[{\"tag_id\":1,\"value\":42.1,\"quality\":192}]}"
curl -X POST http://localhost:8081/v1/locf -H "Content-Type: application/json" -d "{\"tag_ids\":[1],\"at\":\"2026-08-18T18:00:00Z\"}"
curl -X POST http://localhost:8081/v1/range -H "Content-Type: application/json" -d "{\"tag_ids\":[1],\"from\":\"2026-08-18T17:00:00Z\",\"to\":\"2026-08-18T18:00:00Z\"}"
```

- Grafana: http://localhost:3000 (`admin` / `prism`)
- QuestDB console: http://localhost:9001
- Prometheus: http://localhost:9090

Полный контракт: [contracts/openapi.yaml](contracts/openapi.yaml).

## Сессии

Диспетчер гоняет **пары по очереди**: `down -v` → только API и БД пары → прогон → запись → снова `down -v`.

Стартовые пары в `sessions/defaults.yaml`: Go/QuestDB, Go/ClickHouse, C#/QuestDB, Rust/QuestDB.

```powershell
pip install -r sessions/requirements.txt
python sessions/run.py new --why "LOCF и range на тегах"
python sessions/run.py run
python sessions/run.py new --why "Только ClickHouse" --pairs go:clickhouse,csharp:clickhouse,rust:clickhouse --run
```

## Профили

| Профиль | Зачем |
| --- | --- |
| `iot-steady` | Базовый ingest по 250 тегам |
| `high-cardinality` | 10k тегов |
| `burst` | write-spike + out-of-order |
| `write-ceiling` | Потолок записи: HTTP, offer выше конверта |
| `query-mix` | ingest + locf/range/sample/twavg |

```yaml
ingest:
  tag_start: 1
  tag_count: 250
  good_ratio: 0.98   # доля OPC Good (192)
query:
  mix:
    - { op: locf, weight: 50 }
    - { op: range, weight: 40, window: 15m }
```

## Контракт

```
POST /v1/write   { "samples": [{ ts, tag_id, value, quality }] }
POST /v1/read    { mode, tag_ids, at? | from?, to?, step? }
POST /v1/locf    alias mode=locf
POST /v1/range   alias mode=range
GET  /v1/tags    каталог
POST /v1/tags    upsert каталога
GET  /v1/meta
```

`quality`: OPC DA word. 192 Good, 64 Uncertain, 0 Bad.

## Как сравниваем пары

Каждая пара получает один и тот же resource envelope и чистый старт. Эффективность — не «кто красивее в Grafana», а кто при этих лимитах:

1. Держит предложенный ingest без ошибок (`ingest_rate`, `write_errors`).
2. Быстрее отвечает на основные чтения (`locf` / `range` p95).
3. Тратит время в БД, а не в API (`storage_*_p95` vs backend/API p95).
4. Укладывается в CPU/RAM конверта (снимок `docker stats` в конце прогона).

После сессии:

```
sessions/<id>/comparison.yaml          # таблица и ранги
sessions/<id>/session.yaml             # conclusions — черновик той же таблицы
sessions/<id>/pairs/<pair>/pair.yaml   # сырые цифры одной пары
python sessions/run.py compare <id>    # печать scorecard
```

Писать `conclude` имеет смысл уже поверх этих цифр: почему locf у QuestDB быстрее, а write у ClickHouse держит rate.

## Метрики

Те же три слоя: `api`, `backend` (`write` / `locf` / `range` / `sample` / `twavg`), `storage`.
Новый адаптер оборачивается в `Observed` и сразу пишет storage-метрики.

## Порты

| Сервис | Порт |
| --- | --- |
| Go / Python / C# / Rust | 8081 / 8082 / 8083 / 8084 |
| QuestDB HTTP / PG / ILP | 9001 / 8812 / 9009 |
| ClickHouse HTTP / native | 8123 / 9000 |
| TimescaleDB | 5432 |
| InfluxDB | 8086 |
| VictoriaMetrics | 8428 |
| NATS / monitor | 4222 / 8222 |
| Grafana | 3000 |
| Prometheus | 9090 |
