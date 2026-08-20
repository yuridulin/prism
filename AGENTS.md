# Как ведём Prism

Стенд сравнивает пары API + хранилище на одном контракте. Цифры сессии важнее мнения.

## Сессии

- Пары только по очереди. Параллельный прогон ломает сравнение.
- Каждая пара: `down -v` → up только её сервисов → нагрузка → запись → снова wipe.
- Envelope один на все пары, из `sessions/defaults.yaml`. Не крутить CPU/RAM «чтобы красивее выглядело».
- Старт: `python sessions/run.py new --why "..." --duration 3m`, затем `run`.
- Для `query-mix` duration — только фаза чтения. Seed архива идёт раньше и в duration не входит.
- Продолжить с сорвавшейся пары: `--from-pair <slug>`.
- Повторное чтение на уже залитом архиве: `--keep` (без `down -v` и без seed). `ARCHIVE_END` должен совпадать с сидом.
- `wait_ready` обязан переживать обрыв TCP: C# и Rust поднимаются дольше Go. Не сужать `except` до `URLError`.

## Что сравниваем

- `iot-steady`, `high-cardinality`, `burst` — запись при фиксированном offer. locf/range там будут `n/a`.
- `write-ceiling` — потолок записи. HTTP, offer выше конверта; ingest/s и ошибки — это пара, не NATS.
- Чтения — профиль `query-mix`: сначала одинаковый архив на год (частые 1m / редкие 1h), затем locf и range 1..30d на готовой БД. Seed в scorecard не входит.
- Полная матрица: 4 API × 5 БД. Пары по очереди, тот же envelope.
- Scorecard: ingest, ошибки write/query, p95 write/locf/range (backend и storage), CPU/RAM, диск тома БД (`storage_mib`) после той же истории.
- На лёгком ingest (~2k/s) пары не разъедутся по rate — не писать «все одинаковые».
- CPU API в конце часто 0%: снимок `docker stats` после генератора.

## Арена записи

Четыре чемпиона (Go / Python / C# / Rust) соревнуются на `write-ceiling`.
Побеждает выше ingest/s без ошибок; при равенстве — ниже write p95.

Сейчас адаптеры наивные: Timescale — INSERT по строке (Rust даже по одному execute в транзакции),
QuestDB — HTTP `/write`, хотя есть ILP `:9009`, VM — prometheus import.

Разрешено: COPY / UNNEST, ILP TCP, raw Influx line, VM `/api/v1/import`, пулы, reuse HTTP, async insert.
Запрещено: менять OpenAPI, семантику locf/range, envelope, чужой `apps/<backend>`, генератор, профили.
`/readyz` и Observed-метрики должны жить.

Каждый чемпион трогает только свой каталог: `apps/go-api`, `apps/python-api`, `apps/csharp-api`, `apps/rust-api`.

## Контракт

- Один OpenAPI на все API. Диалекты не плодить.
- Запись: `ts` UTC, `tag_id` uint32, `value` float, `quality` OPC DA (192 = Good).
- locf и range — в адаптере хранилища. sample и twavg — в API из результата range.
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
- QuestDB HTTP с хоста: `9001`. Порт `9000` занят ClickHouse native.
- Prometheus: `user: "65534:65534"`. После переноса data-root образ без этого падает.
- Ряды в Timescale / ClickHouse / Influx / VictoriaMetrics хранятся 400 дней — иначе годовой архив для `query-mix` срежется. Prometheus по-прежнему 7d.
