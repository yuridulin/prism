# Как ведём Prism

Стенд сравнивает пары API + хранилище на одном контракте. Цифры сессии важнее мнения.

## Сессии

- По умолчанию **parallel-by-backend**: на каждый API **N параллельных копий** (по одной на storage), общие БД, **один** 5m query на все пары backend-а. `by-backend` — те же БД, но API одно, storage переключается по очереди. Legacy: `--isolated-pairs` / `by-pair`.
- **Volume sets** (суффикс `PRISM_VOLUME_SET`): `data` — query-mix lab (существующие тома `prism_*_data`), `write` — write-ceiling/iot-steady/burst/high-cardinality, `mixed` — sinus-like*. Запись не трогает lab-тома.
- **Seed**: для query-mix на томе `data` сид пропускается, если архив уже есть (locf tag 1 и 9 на `ARCHIVE_END`). Явно: `--keep` или `skip_seed: true`.
- **Preflight** перед `run`: `python sessions/run.py preflight` или автоматически в `run` — write/locf/range на probe-тегах 900001+; все 3 API должны совпасть на каждой БД. `--skip-preflight` только если осознанно.
- Envelope один на все пары, из `sessions/defaults.yaml`. Не крутить CPU/RAM «чтобы красивее выглядело».
- Старт: `python sessions/run.py new --why "..." --duration 3m`, затем `run`.
- Для `query-mix` duration — только фаза чтения. Seed архива идёт раньше и в duration не входит.
- Продолжить с сорвавшейся пары: `--from-pair <slug>`.
- Повторное чтение на уже залитом архиве: `--keep` (без wipe и без seed). `ARCHIVE_END` должен совпадать с сидом.
- `wait_ready` обязан переживать обрыв TCP: C# и Rust поднимаются дольше Go. Не сужать `except` до `URLError`.

## Что сравниваем

- `iot-steady`, `high-cardinality`, `burst` — запись при фиксированном offer. locf/range там будут `n/a`.
- `write-ceiling` — потолок записи. HTTP, offer выше конверта; ingest/s и ошибки — это пара, не NATS.
- Чтения — профиль `query-mix`: сначала одинаковый архив на год (частые 1m / редкие 1h), затем locf и range 1..30d на готовой БД. Seed в scorecard не входит.
- Полная матрица: **3 API × 3 БД** (Go / C# / Rust × Timescale / QuestDB / VictoriaMetrics). Пары по очереди, тот же envelope.
- Scorecard: ingest, ошибки write/query, p95 write/locf/range (backend и storage), CPU/RAM, диск тома БД (`storage_mib`) после той же истории.
- На лёгком ingest (~2k/s) пары не разъедутся по rate — не писать «все одинаковые».
- CPU API в конце часто 0%: снимок `docker stats` после генератора.

## Сокращённая матрица (авг 2026)

После `20260821T172956-query-mix` из стенда убраны **Python API**, **ClickHouse**, **InfluxDB** — код адаптеров в `apps/` остаётся, но compose и сессии их не поднимают.

| Кандидат | Почему выбросили |
| --- | --- |
| **Python API** | На range до ~345k точек pydantic/JSON-сериализация давала секунды и таймауты; Go/C#/Rust на той же БД сходятся — это overhead API, не storage. |
| **InfluxDB** | Худшие чтения на контракте: range p95 ~2–10 s, таймауты на 30d окнах; InfluxQL/line не заточены под сырой range с LOCF. |
| **ClickHouse** | На `write-ceiling` стабильно в хвосте (~41k/s), мутации/тюнинг хрупкие, ни ingest, ни locf/range не лидировали. «Все хвалят ClickHouse» — но не на **этом** контракте (append + locf + raw range без агрегатов). |

## Арена записи

Три чемпиона (Go / C# / Rust) соревнуются на `write-ceiling`.
Побеждает выше ingest/s без ошибок; при равенстве — ниже write p95.

Текущие пути записи (уже не наивные INSERT):

| Стор | Запись | Чтение |
|------|--------|--------|
| Timescale | PostgreSQL **COPY BINARY** (Go `CopyFrom`, C# `BeginBinaryImport`, Rust `FORMAT BINARY`) | locf: `unnest` + `LATERAL LIMIT 1`; range: один SQL (head + tail) |
| QuestDB | **ILP TCP** `:9009`, пул соединений | locf: `LATEST ON` через `/exec`; range: `/exp` CSV **stream** |
| VictoriaMetrics | Influx line `POST /write?precision=ns` (`prism_sample{tag_id,quality}`) | locf и range — один `/api/v1/export/csv` + выбор last/`(from,to]` в адаптере, stream |

Ещё можно крутить: ILP builder без аллокаций, пулы, reuse HTTP, stream CSV, binary COPY. VM `/api/v1/import` — только если не ломает locf/range на `prism_sample{tag_id}`.

Запрещено: менять OpenAPI, семантику locf/range, envelope, чужой `apps/<backend>`, генератор, профили.
`/readyz` и Observed-метрики должны жить.

Приёмка правок адаптера: `python sessions/run.py preflight` — locf/range совпадают у Go/C#/Rust на каждой БД. Сессии write-ceiling/query-mix — цифры эффективности, не ворота корректности.

Каждый чемпион трогает только свой каталог: `apps/go-api`, `apps/csharp-api`, `apps/rust-api`. (`apps/python-api` — вне матрицы, см. выше.)

## Контракт

- Один OpenAPI на все API. Диалекты не плодить.
- Запись: `ts` UTC, `tag_id` uint32, `value` float, `quality` OPC DA (192 = Good).
- locf и range — в адаптере хранилища. Stretch и агрегаты в API нет.
- NATS: `prism.samples`. Новый store оборачивать в `Observed`, чтобы сразу были `storage_*` p95.

## Git

Коммитить журнал сессии:

- `sessions/<id>/session.yaml`
- `sessions/<id>/comparison.yaml`
- `sessions/<id>/pairs/*/pair.yaml`
- `sessions/<id>/pairs/*/compose.env`
- `sessions/catalog.yaml`

Не коммитить: `generator.out`, `__pycache__`, `.env`, тома Docker.

Правку стенда класть в тот же коммит, если без неё прогон не воспроизводится.

## Docker на Windows

- Данные Docker на `D:`, не на системный диск.
- File sharing только `D:\Work`. Весь `D:` шарить нельзя — там диск VM.
- QuestDB HTTP с хоста: `9001`.
- Prometheus: `user: "65534:65534"`. После переноса data-root образ без этого падает.
- Ряды в Timescale / VictoriaMetrics хранятся 400 дней — иначе годовой архив для `query-mix` срежется. Prometheus по-прежнему 7d.
