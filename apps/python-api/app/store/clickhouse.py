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


def _from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _ids(tag_ids: list[int]) -> str:
    return ",".join(str(int(i)) for i in tag_ids)


def _locf_sql(ids: str, at: str, bounded: bool) -> str:
    at_dt = f"toDateTime64('{at}', 3, 'UTC')"
    lower = f" AND s.ts >= {at_dt} - INTERVAL 3 HOUR" if bounded else ""
    return f"""
        SELECT toUnixTimestamp64Milli(max(s.ts)), s.tag_id, argMax(s.value, s.ts), argMax(s.quality, s.ts)
        FROM samples AS s
        WHERE s.tag_id IN ({ids}) AND s.ts <= {at_dt}{lower}
        GROUP BY s.tag_id
    """


def _parse_rows(blob: bytes, carried: bool) -> list[Sample]:
    if len(blob) % _ROW.size:
        raise RuntimeError("clickhouse rowbinary truncated")
    return [
        Sample(ts=_from_ms(ts), tag_id=tag_id, value=float(value), quality=quality, carried=carried)
        for ts, tag_id, value, quality in _ROW.iter_unpack(blob)
    ]


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


class ClickHouseStore:
    name = "clickhouse"

    def __init__(self, url: str, database: str) -> None:
        origin, auth = split_http_url(url)
        self._db = database
        self._http = new_client(timeout=30.0, base_url=origin, auth=auth)
        self._insert_path = (
            f"/?database={quote(database)}&query={quote(_INSERT)}"
            "&async_insert=1&wait_for_async_insert=1&async_insert_busy_timeout_ms=200"
            "&async_insert_max_data_size=1048576"
        )

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
        stamp = _ch_time(at)
        rows = _parse_rows(await self._query_binary(_locf_sql(_ids(tag_ids), stamp, True)), False)
        found = {s.tag_id for s in rows}
        missing = [int(i) for i in tag_ids if int(i) not in found]
        if missing:
            rows.extend(_parse_rows(await self._query_binary(_locf_sql(_ids(missing), stamp, False)), False))
        return rows

    async def range(self, tag_ids: list[int], start: datetime, end: datetime) -> list[Sample]:
        head = [s.as_carried() for s in await self.locf(tag_ids, start)]
        ids = _ids(tag_ids)
        left, right = _ch_time(start), _ch_time(end)
        tail = _parse_rows(
            await self._query_binary(
                f"""
                SELECT toUnixTimestamp64Milli(s.ts), s.tag_id, s.value, s.quality
                FROM samples AS s
                WHERE s.tag_id IN ({ids})
                  AND s.ts > toDateTime64('{left}', 3, 'UTC')
                  AND s.ts <= toDateTime64('{right}', 3, 'UTC')
                """
            ),
            False,
        )
        return _merge_range(tag_ids, head, tail)

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

    async def _query_binary(self, sql: str) -> bytes:
        resp = await self._http.post(
            "/",
            params={"database": self._db, "query": sql + " FORMAT RowBinary"},
        )
        raise_status(resp, "clickhouse query")
        return resp.content
