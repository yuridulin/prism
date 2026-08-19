from datetime import datetime

import clickhouse_connect

from app.models import Sample, Tag


class ClickHouseStore:
    name = "clickhouse"

    def __init__(self, url: str, database: str) -> None:
        self._client = clickhouse_connect.get_client(dsn=url, database=database)

    async def ping(self) -> None:
        self._client.command("SELECT 1")

    async def write(self, samples: list[Sample]) -> None:
        if not samples:
            return
        self._client.insert(
            "samples",
            [(s.ts, s.tag_id, float(s.value), s.quality) for s in samples],
            column_names=["ts", "tag_id", "value", "quality"],
        )

    async def locf(self, tag_ids: list[int], at: datetime) -> list[Sample]:
        rows = self._client.query(
            """
            SELECT ts, tag_id, value, quality
            FROM samples
            WHERE tag_id IN {ids:Array(UInt32)} AND ts <= {at:DateTime64(3)}
            ORDER BY tag_id, ts DESC
            LIMIT 1 BY tag_id
            """,
            parameters={"ids": tag_ids, "at": at},
        ).result_rows
        return [Sample(ts=r[0], tag_id=r[1], value=float(r[2]), quality=int(r[3])) for r in rows]

    async def range(self, tag_ids: list[int], start: datetime, end: datetime) -> list[Sample]:
        rows = self._client.query(
            """
            SELECT ts, tag_id, value, quality, carried FROM (
                SELECT ts, tag_id, value, quality, 1 AS carried
                FROM samples
                WHERE tag_id IN {ids:Array(UInt32)} AND ts <= {start:DateTime64(3)}
                ORDER BY tag_id, ts DESC
                LIMIT 1 BY tag_id
                UNION ALL
                SELECT ts, tag_id, value, quality, 0
                FROM samples
                WHERE tag_id IN {ids:Array(UInt32)} AND ts > {start:DateTime64(3)} AND ts <= {end:DateTime64(3)}
            )
            ORDER BY tag_id, ts
            """,
            parameters={"ids": tag_ids, "start": start, "end": end},
        ).result_rows
        return [
            Sample(ts=r[0], tag_id=r[1], value=float(r[2]), quality=int(r[3]), carried=bool(r[4]))
            for r in rows
        ]

    async def upsert_tags(self, tags: list[Tag]) -> None:
        if not tags:
            return
        self._client.insert("tags", [(t.id, t.name, t.unit) for t in tags], column_names=["id", "name", "unit"])

    async def list_tags(self) -> list[Tag]:
        rows = self._client.query("SELECT id, name, unit FROM tags ORDER BY id").result_rows
        return [Tag(id=r[0], name=r[1], unit=r[2]) for r in rows]

    async def close(self) -> None:
        self._client.close()
