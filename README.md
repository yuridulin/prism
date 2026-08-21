# Prism

Стенд сравнения time-series стеков под задачу **каталога тегов** и append-only рядов.

Запись: `date` (UTC), `id` (uint32), `value` (float), `quality` (OPC DA uint16, 192 = Good).

HTTP как у Datalake `/api/values`, без массива запросов и без `Func`/`Resolution`:

| Вызов | Смысл |
| --- | --- |
| `POST /api/values` + `exact` | LOCF на момент (current / ExactLocf) |
| `POST /api/values` + `old`/`young` | locf на `old` + сырые точки `(old, young]` |
| `PUT /api/values` | пачка точек `{ id, value, date, quality }` |

Stretch и агрегаты в API нет — сравниваются только locf и raw range.

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
curl http://localhost:8081/api/meta
curl -X PUT http://localhost:8081/api/values -H "Content-Type: application/json" -d "[{\"id\":1,\"value\":42.1,\"quality\":192}]"
curl -X POST http://localhost:8081/api/values -H "Content-Type: application/json" -d "{\"tagsId\":[1],\"exact\":\"2026-08-18T18:00:00Z\"}"
curl -X POST http://localhost:8081/api/values -H "Content-Type: application/json" -d "{\"tagsId\":[1],\"old\":\"2026-08-18T17:00:00Z\",\"young\":\"2026-08-18T18:00:00Z\"}"
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
python sessions/run.py new --why "LOCF и range на годовом архиве" --profile query-mix
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
| `query-mix` | год архива (частые/редкие теги), затем locf и range 1..30d |

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

`query-mix` готовит архив, потом только читает:

```yaml
archive:
  span: 365d
  tags:
    - { class: frequent, start: 1, count: 8, period: 1m }
    - { class: rare, start: 9, count: 72, period: 1h }
query:
  mix:
    - { op: locf, weight: 2 }
    - { op: range, weight: 1, window: 1d }
    - { op: range, weight: 1, window: 30d }
```

## Контракт

```
PUT  /api/values   [{ id, value, date?, quality? }]
POST /api/values   { requestKey?, tagsId, exact? | old?, young? }
GET  /api/tags     каталог
POST /api/tags     upsert каталога
GET  /api/meta
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

Сводка прогонов записи/чтения и куда копать дальше: [sessions/REPORT-write-read-2026-08.md](sessions/REPORT-write-read-2026-08.md).

## Метрики

Те же три слоя: `api`, `backend` (`write` / `locf` / `range`), `storage`.
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
