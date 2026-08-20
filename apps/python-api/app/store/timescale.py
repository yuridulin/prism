from datetime import datetime, timezone

import asyncpg

from app.models import Sample, Tag


def _utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


class TimescaleStore:
    name = "timescaledb"

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def _conn(self) -> asyncpg.Pool:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._dsn, min_size=4, max_size=16)
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
        pool = await self._conn()
        rows = await pool.fetch(
            """
            SELECT s.ts, s.tag_id, s.value, s.quality
            FROM unnest($1::int4[]) AS t(tag_id)
            CROSS JOIN LATERAL (
                SELECT ts, tag_id, value, quality
                FROM samples
                WHERE samples.tag_id = t.tag_id AND ts <= $2
                ORDER BY ts DESC
                LIMIT 1
            ) s
            """,
            tag_ids,
            at,
        )
        return [Sample(ts=r["ts"], tag_id=r["tag_id"], value=r["value"], quality=r["quality"]) for r in rows]

    async def range(self, tag_ids: list[int], start: datetime, end: datetime) -> list[Sample]:
        pool = await self._conn()
        rows = await pool.fetch(
            """
            SELECT ts, tag_id, value, quality, carried FROM (
                SELECT s.ts, s.tag_id, s.value, s.quality, true AS carried
                FROM unnest($1::int4[]) AS t(tag_id)
                CROSS JOIN LATERAL (
                    SELECT ts, tag_id, value, quality
                    FROM samples
                    WHERE samples.tag_id = t.tag_id AND ts <= $2
                    ORDER BY ts DESC
                    LIMIT 1
                ) s
                UNION ALL
                SELECT ts, tag_id, value, quality, false
                FROM samples
                WHERE tag_id = ANY($1) AND ts > $2 AND ts <= $3
            ) q
            ORDER BY tag_id, ts
            """,
            tag_ids,
            start,
            end,
        )
        return [
            Sample(ts=r["ts"], tag_id=r["tag_id"], value=r["value"], quality=r["quality"], carried=r["carried"])
            for r in rows
        ]

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
