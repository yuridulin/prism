from datetime import datetime, timedelta
from urllib.parse import urlparse

import clickhouse_connect

from app.models import Point, QueryResult, Sample
from app.store.base import agg_sql, step_seconds


class ClickHouseStore:
    name = "clickhouse"

    def __init__(self, url: str, database: str) -> None:
        parsed = urlparse(url)
        self._client = clickhouse_connect.get_client(
            host=parsed.hostname or "clickhouse",
            port=parsed.port or 8123,
            username=parsed.username or "default",
            password=parsed.password or "",
            database=database,
        )

    async def ping(self) -> None:
        self._client.command("SELECT 1")

    async def write(self, points: list[Point]) -> None:
        if not points:
            return
        self._client.insert(
            "points",
            [[p.ts, p.metric, p.value, p.labels or {}] for p in points],
            column_names=["ts", "metric", "value", "labels"],
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
        conds = ""
        params: dict[str, object] = {
            "metric": metric,
            "start": start,
            "end": end,
        }
        for i, (k, v) in enumerate((labels or {}).items()):
            conds += f" AND labels[%(k{i})s] = %(v{i})s"
            params[f"k{i}"] = k
            params[f"v{i}"] = v
        sql = f"""
            SELECT toStartOfInterval(ts, INTERVAL {step_seconds(step)} SECOND) AS bucket,
                   {agg_sql(agg)}(value) AS value
            FROM points
            WHERE metric = %(metric)s AND ts >= %(start)s AND ts < %(end)s {conds}
            GROUP BY bucket
            ORDER BY bucket
        """
        rows = self._client.query(sql, parameters=params).result_rows
        return QueryResult(
            metric=metric,
            agg=agg,
            step=f"{step_seconds(step)}s",
            points=[Sample(ts=row[0], value=float(row[1])) for row in rows],
        )

    async def latest(self, metric: str, labels: dict[str, str]) -> Point | None:
        conds = ""
        params: dict[str, object] = {"metric": metric}
        for i, (k, v) in enumerate((labels or {}).items()):
            conds += f" AND labels[%(k{i})s] = %(v{i})s"
            params[f"k{i}"] = k
            params[f"v{i}"] = v
        rows = self._client.query(
            f"""
            SELECT ts, metric, value, labels
            FROM points
            WHERE metric = %(metric)s {conds}
            ORDER BY ts DESC
            LIMIT 1
            """,
            parameters=params,
        ).result_rows
        if not rows:
            return None
        ts, name, value, labs = rows[0]
        return Point(ts=ts, metric=name, value=float(value), labels=dict(labs or {}))

    async def close(self) -> None:
        self._client.close()
