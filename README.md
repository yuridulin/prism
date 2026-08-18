# Prism

Стенд для сравнения time-series стеков: один контракт, разные бэкенды и хранилища.

Prism прогоняет одну и ту же нагрузку (ingest + range-query + latest) через:

| Слой | Варианты |
| --- | --- |
| API | Go (`:8081`), Python/FastAPI (`:8082`) |
| Storage | TimescaleDB, ClickHouse, InfluxDB 2, VictoriaMetrics |
| Шина | NATS JetStream |
| Наблюдение | Prometheus + Grafana |
| Нагрузка | генератор + k6 |

Хранилище выбирается переменной `PRISM_STORAGE`, без смены HTTP API.

## Быстрый старт

Нужны Docker и Docker Compose v2.

```powershell
copy .env.example .env
docker compose up -d --build
```

Проверка:

```powershell
curl http://localhost:8081/v1/meta
curl http://localhost:8082/v1/meta
```

Запись и чтение через Go → TimescaleDB:

```powershell
curl -X POST http://localhost:8081/v1/points -H "Content-Type: application/json" -d "{\"points\":[{\"metric\":\"cpu.usage\",\"value\":42.1,\"labels\":{\"host\":\"dev-001\"}}]}"
curl "http://localhost:8081/v1/latest?metric=cpu.usage"
```

UI:

- Grafana: http://localhost:3000 (`admin` / `prism`)
- Prometheus: http://localhost:9090
- NATS monitor: http://localhost:8222

## Сменить хранилище

В `.env`:

```env
GO_API_STORAGE=clickhouse
PYTHON_API_STORAGE=victoriametrics
```

Допустимые значения: `timescaledb`, `clickhouse`, `influxdb`, `victoriametrics`.

Затем `docker compose up -d go-api python-api`.

По умолчанию Go пишет в TimescaleDB, Python — в InfluxDB. Оба подписаны на одну NATS-тему разными queue groups, поэтому одна нагрузка попадает в оба хранилища.

## Нагрузка

Генератор (профиль `load`):

```powershell
docker compose --profile load up generator
```

Или локально в HTTP-режим:

```powershell
cd ingest/generator
pip install -r requirements.txt
python generator.py --target http --http-url http://localhost:8081 --rate 1000 --duration 30
```

k6:

```powershell
k6 run -e BASE_URL=http://localhost:8081 -e RATE=200 bench/k6/ingest.js
k6 run -e BASE_URL=http://localhost:8081 bench/k6/query.js
k6 run -e BASE_URL=http://localhost:8082 bench/k6/query.js
```

Сравнивать удобно по дашборду **Prism overview** в Grafana и по `prism_*` метрикам обоих API.

## Контракт

См. [contracts/openapi.yaml](contracts/openapi.yaml).

```
POST /v1/points     пачка точек
GET  /v1/query      range + downsample (avg|min|max|sum|count)
GET  /v1/latest     последняя точка
GET  /v1/meta       какой backend/storage сейчас активен
```

Модель точки:

```json
{
  "ts": "2026-08-18T16:00:00Z",
  "metric": "cpu.usage",
  "value": 42.5,
  "labels": { "host": "dev-001", "site": "lab" }
}
```

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

## Структура

```
apps/go-api          Go HTTP + адаптеры всех 4 хранилищ + NATS
apps/python-api      то же на FastAPI
ingest/generator     синтетика (NATS или HTTP)
bench/k6             ingest / query сценарии
contracts/           OpenAPI
infra/               init SQL, Prometheus, Grafana
```

Дальше по стенду: ещё один язык API, Kafka как альтернатива NATS, k8s-манифесты рядом с Compose, отдельные сценарии (высокая кардинальность, out-of-order, backfill).
