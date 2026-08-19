import json
import struct
from datetime import datetime, timezone
from urllib.parse import quote

import httpx

from app.models import Sample, Tag
from app.store.net import new_client, raise_status, split_http_url, unix_ms

_ROW = struct.Struct("<qIfH")
_INSERT = (
    "INSERT INTO samples (ts, tag_id, value, quality) "
    "SELECT fromUnixTimestamp64Milli(ts), tag_id, value, quality "
    "FROM input('ts Int64, tag_id UInt32, value Float32, quality UInt16') FORMAT RowBinary"
)


def _ch_time(ts: datetime) -> str:
    utc = ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return utc.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _parse_ts(raw) -> datetime:
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    text = str(raw).replace("T", " ").replace("Z", "")
    if "." in text:
        dt = datetime.strptime(text[:26], "%Y-%m-%d %H:%M:%S.%f")
    else:
        dt = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
    return dt.replace(tzinfo=timezone.utc)


class ClickHouseStore:
    name = "clickhouse"

    def __init__(self, url: str, database: str) -> None:
        origin, auth = split_http_url(url)
        self._db = database
        self._http = new_client(timeout=30.0, base_url=origin, auth=auth)
        self._insert_path = f"/?database={quote(database)}&query={quote(_INSERT)}"

    async def ping(self) -> None:
        await self._query("SELECT 1")

    async def write(self, samples: list[Sample]) -> None:
        if not samples:
            return
        payload = bytearray(18 * len(samples))
        offset = 0
        for s in samples:
            _ROW.pack_into(payload, offset, unix_ms(s.ts), int(s.tag_id), float(s.value), int(s.quality))
            offset += 18
        blob = bytes(payload)
        last: Exception | None = None
        for attempt in range(2):
            try:
                resp = await self._http.post(
                    self._insert_path,
                    content=blob,
                    headers={"Content-Type": "application/octet-stream"},
                )
                raise_status(resp, "clickhouse write")
                return
            except httpx.TransportError as exc:
                last = exc
                if attempt:
                    break
        raise last or RuntimeError("clickhouse write failed")

    async def locf(self, tag_ids: list[int], at: datetime) -> list[Sample]:
        ids = ",".join(str(int(i)) for i in tag_ids)
        rows = await self._query(
            f"""
            SELECT ts, tag_id, value, quality
            FROM samples
            WHERE tag_id IN ({ids}) AND ts <= '{_ch_time(at)}'
            ORDER BY tag_id, ts DESC
            LIMIT 1 BY tag_id
            """
        )
        return [Sample(ts=_parse_ts(r[0]), tag_id=int(r[1]), value=float(r[2]), quality=int(r[3])) for r in rows]

    async def range(self, tag_ids: list[int], start: datetime, end: datetime) -> list[Sample]:
        ids = ",".join(str(int(i)) for i in tag_ids)
        left, right = _ch_time(start), _ch_time(end)
        rows = await self._query(
            f"""
            SELECT ts, tag_id, value, quality, carried FROM (
                SELECT ts, tag_id, value, quality, 1 AS carried
                FROM samples
                WHERE tag_id IN ({ids}) AND ts <= '{left}'
                ORDER BY tag_id, ts DESC
                LIMIT 1 BY tag_id
                UNION ALL
                SELECT ts, tag_id, value, quality, 0
                FROM samples
                WHERE tag_id IN ({ids}) AND ts > '{left}' AND ts <= '{right}'
            )
            ORDER BY tag_id, ts
            """
        )
        return [
            Sample(ts=_parse_ts(r[0]), tag_id=int(r[1]), value=float(r[2]), quality=int(r[3]), carried=bool(r[4]))
            for r in rows
        ]

    async def upsert_tags(self, tags: list[Tag]) -> None:
        if not tags:
            return
        body = "\n".join(json.dumps({"id": t.id, "name": t.name, "unit": t.unit}, separators=(",", ":")) for t in tags)
        resp = await self._http.post(
            "/",
            params={"database": self._db, "query": "INSERT INTO tags (id, name, unit) FORMAT JSONEachRow"},
            content=body,
        )
        raise_status(resp, "clickhouse tags")

    async def list_tags(self) -> list[Tag]:
        rows = await self._query("SELECT id, name, unit FROM tags ORDER BY id")
        return [Tag(id=int(r[0]), name=str(r[1]), unit=str(r[2])) for r in rows]

    async def close(self) -> None:
        await self._http.aclose()

    async def _query(self, sql: str) -> list:
        resp = await self._http.post(
            "/",
            params={"database": self._db, "query": sql + " FORMAT JSONCompact"},
        )
        raise_status(resp, "clickhouse query")
        return resp.json().get("data") or []
