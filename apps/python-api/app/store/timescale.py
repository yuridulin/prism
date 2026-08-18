import json
from datetime import datetime, timedelta

import asyncpg

from app.models import Point, QueryResult, Sample
from app.store.base import agg_sql, step_seconds


class TimescaleStore:
    name = "timescaledb"

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def _ensure(self) -> asyncpg.Pool:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=8)
        return self._pool

    async def ping(self) -> None:
        pool = await self._ensure()
        await pool.execute("SELECT 1")

    async def write(self, points: list[Point]) -> None:
        if not points:
            return
        pool = await self._ensure()
        await pool.executemany(
            "INSERT INTO points (ts, metric, value, labels) VALUES ($1, $2, $3, $4::jsonb)",
            [
                (p.ts, p.metric, p.value, json.dumps(p.labels or {}))
                for p in points
            ],
        )

    async def query(
        self,
        metric: str,
        start: datetime,
        end: datetime,
        step: timedelta,
        agg: str,
        labels: dict[str, str],
    ) -> QueryResult:
        pool = await self._ensure()
        sql = f"""
            SELECT time_bucket($1::interval, ts) AS bucket, {agg_sql(agg)}(value) AS value
            FROM points
            WHERE metric = $2 AND ts >= $3 AND ts < $4 AND labels @> $5::jsonb
            GROUP BY bucket
            ORDER BY bucket
        """
        rows = await pool.fetch(
            sql,
            f"{step_seconds(step)} seconds",
            metric,
            start,
            end,
            json.dumps(labels or {}),
        )
        return QueryResult(
            metric=metric,
            agg=agg,
            step=f"{step_seconds(step)}s",
            points=[Sample(ts=r["bucket"], value=float(r["value"])) for r in rows],
        )

    async def latest(self, metric: str, labels: dict[str, str]) -> Point | None:
        pool = await self._ensure()
        row = await pool.fetchrow(
            """
            SELECT ts, metric, value, labels
            FROM points
            WHERE metric = $1 AND labels @> $2::jsonb
            ORDER BY ts DESC
            LIMIT 1
            """,
            metric,
            json.dumps(labels or {}),
        )
        if row is None:
            return None
        raw = row["labels"]
        if isinstance(raw, str):
            raw = json.loads(raw)
        return Point(ts=row["ts"], metric=row["metric"], value=float(row["value"]), labels=dict(raw or {}))

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
