from datetime import datetime, timedelta, timezone

import asyncpg

from app.models import Sample, Tag

_LOCF_LOOKBACK = timedelta(hours=3)

_LOCF_SQL = """
SELECT s.ts, s.tag_id, s.value, s.quality
FROM unnest($1::int4[]) AS t(tag_id)
CROSS JOIN LATERAL (
    SELECT ts, tag_id, value, quality
    FROM samples
    WHERE samples.tag_id = t.tag_id AND ts <= $2 AND ts >= $3
    ORDER BY ts DESC
    LIMIT 1
) s
"""

_LOCF_UNBOUNDED_SQL = """
SELECT s.ts, s.tag_id, s.value, s.quality
FROM unnest($1::int4[]) AS t(tag_id)
CROSS JOIN LATERAL (
    SELECT ts, tag_id, value, quality
    FROM samples
    WHERE samples.tag_id = t.tag_id AND ts <= $2
    ORDER BY ts DESC
    LIMIT 1
) s
"""

_RANGE_SQL = """
SELECT ts, tag_id, value, quality
FROM samples
WHERE tag_id = ANY($1) AND ts > $2 AND ts <= $3
ORDER BY tag_id, ts
"""


def _utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _merge_range(tag_ids: list[int], head: list[Sample], tail: list[Sample]) -> list[Sample]:
    buckets: dict[int, list[Sample]] = {int(t): [] for t in tag_ids}
    extra: list[Sample] = []
    for sample in head:
        buckets.setdefault(sample.tag_id, []).append(sample)
    for sample in tail:
        if sample.tag_id in buckets:
            buckets[sample.tag_id].append(sample)
        else:
            extra.append(sample)
    out: list[Sample] = []
    for tag_id in tag_ids:
        out.extend(buckets[int(tag_id)])
    out.extend(extra)
    return out


class TimescaleStore:
    name = "timescaledb"

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def _conn(self) -> asyncpg.Pool:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                self._dsn,
                min_size=4,
                max_size=16,
                server_settings={"TimeZone": "UTC"},
            )
        return self._pool

    async def ping(self) -> None:
        pool = await self._conn()
        await pool.execute("SELECT 1")

    async def write(self, samples: list[Sample]) -> None:
        if not samples:
            return
        pool = await self._conn()
        records = [
            (_utc(s.ts), int(s.tag_id), float(s.value), int(s.quality)) for s in samples
        ]
        await pool.copy_records_to_table(
            "samples",
            records=records,
            columns=["ts", "tag_id", "value", "quality"],
        )

    async def locf(self, tag_ids: list[int], at: datetime) -> list[Sample]:
        at = _utc(at)
        ids = [int(i) for i in tag_ids]
        rows = await self._locf(ids, at, True)
        found = {s.tag_id for s in rows}
        missing = [i for i in ids if i not in found]
        if missing:
            rows.extend(await self._locf(missing, at, False))
        return rows

    async def _locf(self, tag_ids: list[int], at: datetime, bounded: bool) -> list[Sample]:
        pool = await self._conn()
        if bounded:
            raw = await pool.fetch(_LOCF_SQL, tag_ids, at, at - _LOCF_LOOKBACK)
        else:
            raw = await pool.fetch(_LOCF_UNBOUNDED_SQL, tag_ids, at)
        return [
            Sample(
                ts=_utc(r["ts"]), tag_id=int(r["tag_id"]), value=float(r["value"]), quality=int(r["quality"])
            )
            for r in raw
        ]

    async def range(self, tag_ids: list[int], start: datetime, end: datetime) -> list[Sample]:
        start, end = _utc(start), _utc(end)
        ids = [int(i) for i in tag_ids]
        head = [s.as_carried() for s in await self.locf(ids, start)]
        pool = await self._conn()
        raw = await pool.fetch(_RANGE_SQL, ids, start, end)
        tail = [
            Sample(
                ts=_utc(r["ts"]), tag_id=int(r["tag_id"]), value=float(r["value"]), quality=int(r["quality"])
            )
            for r in raw
        ]
        return _merge_range(ids, head, tail)

    async def upsert_tags(self, tags: list[Tag]) -> None:
        pool = await self._conn()
        await pool.executemany(
            """
            INSERT INTO tags (id, name, unit) VALUES ($1, $2, $3)
            ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, unit = EXCLUDED.unit
            """,
            [(t.id, t.name, t.unit) for t in tags],
        )

    async def list_tags(self) -> list[Tag]:
        pool = await self._conn()
        rows = await pool.fetch("SELECT id, name, COALESCE(unit, '') AS unit FROM tags ORDER BY id")
        return [Tag(id=r["id"], name=r["name"], unit=r["unit"]) for r in rows]

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
