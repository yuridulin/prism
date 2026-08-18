# Prism

Стенд для сравнения time-series стеков: один контракт, декларативные профили нагрузки, три слоя метрик.

| Слой | Варианты |
| --- | --- |
| API | Go (`:8081`), Python/FastAPI (`:8082`) |
| Storage | TimescaleDB, ClickHouse, InfluxDB 2, VictoriaMetrics |
| Шина | NATS JetStream |
| Наблюдение | Prometheus + Grafana + native exporters |
| Нагрузка | YAML-профили → генератор или k6 |

Хранилище выбирается `PRISM_STORAGE` / `GO_API_STORAGE` / `PYTHON_API_STORAGE`.

## Быстрый старт

Нужны Docker и Docker Compose v2.

```powershell
copy .env.example .env
docker compose up -d --build
```

```powershell
curl http://localhost:8081/v1/meta
curl -X POST http://localhost:8081/v1/points -H "Content-Type: application/json" -d "{\"points\":[{\"metric\":\"cpu.usage\",\"value\":42.1,\"labels\":{\"host\":\"dev-001\"}}]}"
curl -X POST http://localhost:8081/v1/latest -H "Content-Type: application/json" -d "{\"metric\":\"cpu.usage\"}"
```

- Grafana: http://localhost:3000 (`admin` / `prism`)
- Prometheus: http://localhost:9090

## Профили нагрузки

Источник правды — `profiles/*.yaml`. Новый сценарий = новый файл, без правок генератора.

| Профиль | Зачем |
| --- | --- |
| `iot-steady` | Базовый непрерывный ingest |
| `high-cardinality` | Много серий, давление на индексы и теги |
| `burst` | Короткий write-spike + небольшой out-of-order |
| `query-mix` | Ingest + range/latest по HTTP |

```powershell
$env:LOAD_PROFILE="query-mix"
docker compose --profile load run --rm generator
```

Локально:

```powershell
pip install -r ingest/generator/requirements.txt
python ingest/generator/generator.py --list
python ingest/generator/generator.py --profile burst --transport http --http-url http://localhost:8081
python bench/run.py --profile query-mix --mode generator --base-url http://localhost:8081
python bench/run.py --profile query-mix --mode k6 --base-url http://localhost:8081
```

Поля профиля (расширяется без смены кода):

```yaml
name: example
transport: nats          # nats | http
duration: 0              # секунды, 0 = пока не остановят
ingest:
  enabled: true
  rate: 2000             # points/s
  batch: 100
  workers: 1
  metrics: [cpu.usage]
  labels:
    host: { prefix: dev-, count: 50, width: 3 }
    site: { values: [lab] }
  out_of_order: 0.0
  late_ms: 0
query:
  enabled: false
  rate: 40
  mix:
    - { op: query, weight: 70, window: 15m, step: 1m, agg: avg }
    - { op: latest, weight: 30 }
```

`TARGET` / `--transport` и `DURATION` перекрывают профиль, если заданы.

## Сессии

Прогон — это запись в `sessions/`: когда, что, зачем, результаты, выводы. Их будет много; каталог — `sessions/catalog.yaml`.

Стартовые характеристики новой сессии лежат в `sessions/defaults.yaml`: 5m, `iot-steady`, общий resource envelope, список пар.

Диспетчер гоняет **пары по очереди**. На каждую: `down -v` → только её API и БД → прогон → запись → снова `down -v`. Соседние стеки не стартуют.

```powershell
pip install -r sessions/requirements.txt
python sessions/run.py new --why "Базовый write-path"
python sessions/run.py run
python sessions/run.py new --why "Только ClickHouse" --pairs go:clickhouse,python:clickhouse --run
python sessions/run.py run --from-pair python-influxdb
python sessions/run.py list
python sessions/run.py conclude <id> --text "Go/Timescale держит rate, Python/Influx хуже по p95."
```

## Контракт API

Канонические методы — POST. GET оставлен для отладки. Ошибки всегда `{ "error": { "code", "message" } }`.

```
POST /v1/points     { "points": [...] }           → { "written": N }
POST /v1/query      { metric, from, to, step, agg, labels }
POST /v1/latest     { metric, labels }
GET  /v1/meta       backend, storage, contract, ops
```

Коды ошибок: `invalid_request`, `not_found`, `storage_unavailable`, `storage_error`.

Полный контракт: [contracts/openapi.yaml](contracts/openapi.yaml).

## Метрики

Три слоя с одинаковыми именами в Go и Python. Новый адаптер хранилища оборачивается в `Observed` / `ObservedStore` и сразу пишет storage-метрики.

| Слой | Метрики | Что измеряет |
| --- | --- | --- |
| `api` | `prism_api_requests_total`, `prism_api_request_duration_seconds` | HTTP route/method/status |
| `backend` | `prism_backend_ops_total`, `prism_backend_op_duration_seconds`, `prism_backend_items_total` | write/query/latest, source=`http`\|`nats` |
| `storage` | `prism_storage_ops_total`, `prism_storage_op_duration_seconds`, `prism_storage_up` | вызов адаптера БД |

Лейблы: `backend`, `storage`, `op`, `source`, `result` (`ok` / `error` / `not_found`).

Native scrape рядом:

- TimescaleDB — postgres-exporter
- ClickHouse — `/metrics` на `:9363`
- InfluxDB — `/metrics`
- VictoriaMetrics — `/metrics`
- NATS — monitor `:8222`

Новая БД: адаптер + scrape job в `infra/prometheus/prometheus.yml`. Новый op: те же `ObserveBackend` / `observe_backend`.

## Сменить хранилище

```env
GO_API_STORAGE=clickhouse
PYTHON_API_STORAGE=victoriametrics
```

Значения: `timescaledb`, `clickhouse`, `influxdb`, `victoriametrics`.

В сессии пары идут по одной. Полный `docker compose up` по-прежнему поднимает весь стенд для ручной отладки.

## Порты

| Сервис | Порт |
| --- | --- |
| Go API | 8081 |
| Python API | 8082 |
| TimescaleDB | 5432 |
| ClickHouse HTTP / native | 8123 / 9000 |
| InfluxDB | 8086 |
| VictoriaMetrics | 8428 |
| NATS / monitor | 4222 / 8222 |
| Grafana | 3000 |
| Prometheus | 9090 |
