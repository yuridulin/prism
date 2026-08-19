from datetime import datetime
from urllib.parse import quote

import httpx

from app.models import Sample, Tag


def _qdb_time(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class QuestDBStore:
    name = "questdb"

    def __init__(self, url: str) -> None:
        self._url = url.rstrip("/")
        self._http = httpx.Client(timeout=30.0)

    async def ping(self) -> None:
        self._exec(
            "CREATE TABLE IF NOT EXISTS samples (ts TIMESTAMP, tag_id INT, value FLOAT, quality SHORT) timestamp(ts) PARTITION BY DAY WAL"
        )
        self._exec("CREATE TABLE IF NOT EXISTS tags (id INT, name SYMBOL, unit SYMBOL)")
        self._exec("SELECT 1")

    async def write(self, samples: list[Sample]) -> None:
        if not samples:
            return
        lines = []
        for s in samples:
            ns = int(s.ts.timestamp() * 1_000_000_000)
            lines.append(f"samples tag_id={s.tag_id}i,value={s.value},quality={s.quality}i {ns}")
        resp = self._http.post(f"{self._url}/write?precision=n", content="\n".join(lines) + "\n")
        resp.raise_for_status()

    async def locf(self, tag_ids: list[int], at: datetime) -> list[Sample]:
        ids = ",".join(str(i) for i in tag_ids)
        data = self._exec(
            f"SELECT ts, tag_id, value, quality FROM samples "
            f"WHERE tag_id IN ({ids}) AND ts <= '{_qdb_time(at)}' "
            f"LATEST ON ts PARTITION BY tag_id"
        )
        return self._samples(data, False)

    async def range(self, tag_ids: list[int], start: datetime, end: datetime) -> list[Sample]:
        ids = ",".join(str(i) for i in tag_ids)
        data = self._exec(
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
            ORDER BY tag_id, ts
            """
        )
        return self._samples(data, True)

    async def upsert_tags(self, tags: list[Tag]) -> None:
        for tag in tags:
            name = tag.name.replace("'", "''")
            unit = tag.unit.replace("'", "''")
            self._exec(f"INSERT INTO tags (id, name, unit) VALUES ({tag.id}, '{name}', '{unit}')")

    async def list_tags(self) -> list[Tag]:
        data = self._exec("SELECT id, name, unit FROM tags ORDER BY id")
        return [Tag(id=int(row[0]), name=str(row[1]), unit=str(row[2])) for row in data.get("dataset") or []]

    async def close(self) -> None:
        self._http.close()

    def _exec(self, query: str) -> dict:
        resp = self._http.get(f"{self._url}/exec?query={quote(query)}")
        resp.raise_for_status()
        data = resp.json()
        if data.get("error"):
            raise RuntimeError(data["error"])
        return data

    def _samples(self, data: dict, has_carried: bool) -> list[Sample]:
        out: list[Sample] = []
        for row in data.get("dataset") or []:
            ts = row[0]
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            sample = Sample(ts=ts, tag_id=int(row[1]), value=float(row[2]), quality=int(row[3]))
            if has_carried and len(row) > 4:
                sample.carried = bool(row[4])
            out.append(sample)
        return out
