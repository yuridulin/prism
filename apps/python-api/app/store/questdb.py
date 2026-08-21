import asyncio
import csv
import io
import socket
from datetime import datetime, timezone

from app.models import Sample, Tag
from app.store.net import ilp_float, new_client, parse_hostport, raise_status, unix_ns


def _qdb_time(ts: datetime) -> str:
    utc = ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _symbol_ids(tag_ids: list[int]) -> str:
    return ",".join(f"'{i}'" for i in tag_ids)


def _parse_qdb_csv(text: str) -> list[Sample]:
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        return []
    idx = {name.strip().lower(): i for i, name in enumerate(header)}
    ts_i, tag_i, val_i, q_i = idx["ts"], idx["tag_id"], idx["value"], idx["quality"]
    c_i = idx.get("carried")
    out: list[Sample] = []
    for row in reader:
        if max(ts_i, tag_i, val_i, q_i) >= len(row):
            continue
        ts = row[ts_i]
        try:
            stamp = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            sample = Sample(
                ts=stamp, tag_id=int(row[tag_i]), value=float(row[val_i]), quality=int(float(row[q_i]))
            )
        except (TypeError, ValueError):
            continue
        if c_i is not None and c_i < len(row):
            sample.carried = str(row[c_i]).lower() in {"true", "t", "1"}
        out.append(sample)
    return out


async def _close_writer(writer: asyncio.StreamWriter) -> None:
    try:
        writer.close()
        await writer.wait_closed()
    except Exception:
        pass


class _IlpPool:
    def __init__(self, addr: str) -> None:
        self._host, self._port = parse_hostport(addr, 9009)
        self._idle: asyncio.Queue[asyncio.StreamWriter] = asyncio.Queue()

    async def write(self, payload: bytes) -> None:
        last: Exception | None = None
        for attempt in range(2):
            writer: asyncio.StreamWriter | None = None
            try:
                writer = await self._rent()
                writer.write(payload)
                await writer.drain()
                self._idle.put_nowait(writer)
                return
            except Exception as exc:
                last = exc
                if writer is not None:
                    await _close_writer(writer)
                if attempt:
                    break
        raise RuntimeError(f"questdb ilp write failed: {last}") from last

    async def ping(self) -> None:
        writer = await self._connect()
        self._idle.put_nowait(writer)

    async def close(self) -> None:
        while not self._idle.empty():
            await _close_writer(self._idle.get_nowait())

    async def _rent(self) -> asyncio.StreamWriter:
        while not self._idle.empty():
            writer = self._idle.get_nowait()
            if not writer.is_closing():
                return writer
            await _close_writer(writer)
        return await self._connect()

    async def _connect(self) -> asyncio.StreamWriter:
        _, writer = await asyncio.open_connection(self._host, self._port)
        sock = writer.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 256 * 1024)
        return writer


class QuestDBStore:
    name = "questdb"

    def __init__(self, url: str, ilp: str = "questdb:9009") -> None:
        self._url = url.rstrip("/")
        self._http = new_client(timeout=30.0, base_url=self._url)
        self._ilp = _IlpPool(ilp)

    async def ping(self) -> None:
        await self._exec(
            "CREATE TABLE IF NOT EXISTS samples (ts TIMESTAMP, tag_id SYMBOL CAPACITY 256 CACHE INDEX, value FLOAT, quality SHORT) timestamp(ts) PARTITION BY DAY WAL"
        )
        await self._exec("CREATE TABLE IF NOT EXISTS tags (id INT, name SYMBOL, unit SYMBOL)")
        await self._exec("SELECT 1")
        await self._ilp.ping()

    async def write(self, samples: list[Sample]) -> None:
        if not samples:
            return
        parts = [
            f"samples tag_id={s.tag_id}i,value={ilp_float(s.value)},quality={s.quality}i {unix_ns(s.ts)}\n"
            for s in samples
        ]
        await self._ilp.write("".join(parts).encode("ascii"))

    async def locf(self, tag_ids: list[int], at: datetime) -> list[Sample]:
        ids = _symbol_ids(tag_ids)
        data = await self._exec(
            f"SELECT ts, tag_id, value, quality FROM samples "
            f"WHERE tag_id IN ({ids}) AND ts <= '{_qdb_time(at)}' "
            f"LATEST ON ts PARTITION BY tag_id"
        )
        return self._samples(data, False)

    async def range(self, tag_ids: list[int], start: datetime, end: datetime) -> list[Sample]:
        ids = _symbol_ids(tag_ids)
        return await self._exp(
            f"""
            SELECT ts, tag_id, value, quality, carried FROM (
              SELECT ts, tag_id, value, quality, true AS carried
              FROM samples
              WHERE tag_id IN ({ids}) AND ts <= '{_qdb_time(start)}'
              LATEST ON ts PARTITION BY tag_id
              UNION ALL
              SELECT ts, tag_id, value, quality, false
              FROM samples
              WHERE tag_id IN ({ids}) AND ts > '{_qdb_time(start)}' AND ts <= '{_qdb_time(end)}'
            )
            """
        )

    async def upsert_tags(self, tags: list[Tag]) -> None:
        for tag in tags:
            name = tag.name.replace("'", "''")
            unit = tag.unit.replace("'", "''")
            await self._exec(f"INSERT INTO tags (id, name, unit) VALUES ({tag.id}, '{name}', '{unit}')")

    async def list_tags(self) -> list[Tag]:
        data = await self._exec("SELECT id, name, unit FROM tags ORDER BY id")
        return [Tag(id=int(row[0]), name=str(row[1]), unit=str(row[2])) for row in data.get("dataset") or []]

    async def close(self) -> None:
        await self._ilp.close()
        await self._http.aclose()

    async def _exec(self, query: str) -> dict:
        resp = await self._http.get("/exec", params={"query": query})
        raise_status(resp, "questdb exec")
        data = resp.json()
        if data.get("error"):
            raise RuntimeError(data["error"])
        return data

    async def _exp(self, query: str) -> list[Sample]:
        resp = await self._http.get("/exp", params={"query": query})
        raise_status(resp, "questdb exp")
        return _parse_qdb_csv(resp.text)

    def _samples(self, data: dict, has_carried: bool) -> list[Sample]:
        out: list[Sample] = []
        for row in data.get("dataset") or []:
            if len(row) < 4:
                continue
            ts = row[0]
            try:
                if isinstance(ts, str):
                    ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                elif not isinstance(ts, datetime):
                    continue
                sample = Sample(ts=ts, tag_id=int(row[1]), value=float(row[2]), quality=int(row[3]))
            except (TypeError, ValueError):
                continue
            if has_carried and len(row) > 4:
                sample.carried = bool(row[4])
            out.append(sample)
        return out
