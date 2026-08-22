# Как ведём Prism

Стенд сравнивает пары API + хранилище на одном контракте. Цифры сессии важнее мнения.

## Сессии

- По умолчанию **parallel-by-backend**: на каждый API **N параллельных копий** (по одной на storage), общие БД, **один** 5m query на все пары backend-а. `by-backend` — те же БД, но API одно, storage переключается по очереди. Legacy: `--isolated-pairs` / `by-pair`.
- **Volume sets** (суффикс `PRISM_VOLUME_SET`): `data` — query-mix lab (существующие тома `prism_*_data`), `write` — write-ceiling/iot-steady/burst/high-cardinality, `mixed` — sinus-like*. Запись не трогает lab-тома.
- **Seed**: для query-mix на томе `data` сид пропускается, если архив уже есть (locf tag 1 и 9 на `ARCHIVE_END`). Явно: `--keep` или `skip_seed: true`.
- **Preflight** перед `run`: `python sessions/run.py preflight` или автоматически в `run` — write/locf/range на probe-тегах 900001+; ответы C# на Timescale и VM должны совпасть с `sessions/fixtures/contract-probe.yaml`. `--skip-preflight` только если осознанно.
- Envelope один на все пары, из `sessions/defaults.yaml`. Не крутить CPU/RAM «чтобы красивее выглядело».
- Старт: `python sessions/run.py new --why "..." --duration 3m`, затем `run`.
- Для `query-mix` duration — только фаза чтения. Seed архива идёт раньше и в duration не входит.
- Продолжить с сорвавшейся пары: `--from-pair <slug>`.
- Повторное чтение на уже залитом архиве: `--keep` (без wipe и без seed). `ARCHIVE_END` должен совпадать с сидом.
- `wait_ready` обязан переживать обрыв TCP: C# поднимается дольше Go. Не сужать `except` до `URLError`.

## Что сравниваем

- `iot-steady`, `high-cardinality`, `burst` — запись при фиксированном offer. locf/range там будут `n/a`.
- `write-ceiling` — потолок записи. HTTP, offer выше конверта; ingest/s и ошибки — это пара, не NATS.
- Чтения — профиль `query-mix`: сначала одинаковый архив на год (частые 1m / редкие 1h), затем locf и range 1..30d на готовой БД. Seed в scorecard не входит.
- Арена по умолчанию: **C# × Timescale / VictoriaMetrics**. Go / Rust / QuestDB вне сравнения (предпочитаемый стек C#; QuestDB не выиграл ни запись, ни range). `--pairs` всё ещё может поднять старые комбинации.
- Scorecard: ingest, ошибки write/query, p95 write/locf/range (backend и storage), CPU/RAM, диск тома БД (`storage_mib`) после той же истории.
- На лёгком ingest (~2k/s) пары не разъедутся по rate — не писать «все одинаковые».
- CPU API в конце часто 0%: снимок `docker stats` после генератора.

## Сокращённая матрица (авг 2026)

После `20260821T172956-query-mix` из стенда убраны **Python API**, **ClickHouse**, **InfluxDB** — код адаптеров в `apps/` остаётся, но compose и сессии их не поднимают.

| Кандидат | Почему выбросили |
| --- | --- |
| **Python API** | На range до ~345k точек pydantic/JSON-сериализация давала секунды и таймауты. |
| **InfluxDB** | Худшие чтения на контракте: range p95 ~2–10 s, таймауты на 30d окнах. |
| **ClickHouse** | На `write-ceiling` стабильно в хвосте; ни ingest, ни locf/range не лидировали. |
| **QuestDB** | На C# range хуже VM, диск как у Timescale. |
| **Go / Rust API** | Языковой эксперимент; дальше предпочитаемый стек **C#**. |

## Арена записи

C# на Timescale и VictoriaMetrics. Побеждает выше ingest/s без ошибок; при равенстве — ниже write p95.

Текущие пути (append-only, без `ON CONFLICT` на рядах):

| Стор | Запись | Чтение |
|------|--------|--------|
| Timescale | PostgreSQL **COPY BINARY** (`BeginBinaryImport`) | locf: `unnest` + `LATERAL LIMIT 1`; range: head LATERAL + tail `(old, young]`; sample: `generate_series` + LATERAL last |
| VictoriaMetrics | Influx line `POST /write?precision=ns` (`prism_sample{tag_id,quality}`) | locf: MetricsQL `last_over_time`/`tlast_over_time`; range: locf-seed + export `(old, young]`; sample: `query_range` |

Запрещено: менять OpenAPI, семантику locf/range, envelope, генератор, профили, `apps/go-api` / `apps/rust-api` в этой арене.
`/readyz` и Observed-метрики должны жить.

Приёмка: `python sessions/run.py preflight` — locf/range C# на Timescale и VM совпадают с фикстурой. Сессии write-ceiling/query-mix — цифры эффективности, не ворота корректности.

Править только `apps/csharp-api` (остальные адаптеры в `apps/` не трогать, пока не просят `--pairs`).

## Контракт

- Один OpenAPI на все API. Диалекты не плодить.
- Запись: `ts` UTC, `tag_id` uint32, `value` float, `quality` OPC DA (192 = Good).
- locf: последняя точка `ts ≤ exact`; в HTTP **сырой ts** (как Sinus ExactLocf), quality через CarryQuality (200 Good_LOCF / 64 LastUsable).
- range: locf-seed на old (сырой ts) + точки `(old, young]`. VM seed без lookback-дыры.
- sample: `old`+`young`+`resolution` — сетка в адаптере (Timescale LATERAL, VM query_range). Stretch в API только если стор не умеет.
- Сравнение чтений — **`storage_*` p95** (данные в памяти). JSON/HTTP — `api_p95`.
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
- C# в конверте 1 CPU / 512M: workstation GC (`DOTNET_gcServer=0`), `GCHeapHardLimit` ~384MiB, `ThreadPool.SetMinThreads(16, 16)`. Иначе Server GC и пул из 1 воркера сажают range.
- Если `docker compose build csharp-api` зависает на `dotnet publish` в Linux SDK, собрать runtime-образ с хоста и `PRISM_NO_BUILD=1` (run.py снимает `--build`).
