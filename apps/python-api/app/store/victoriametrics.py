import json
from datetime import datetime

from app.models import Sample, Tag
from app.store.catalog import CatalogMem
from app.store.net import ilp_float, new_client, raise_status, unix_ns

_HTTP = None


def _http():
    global _HTTP
    if _HTTP is None or _HTTP.is_closed:
        _HTTP = new_client(timeout=15.0)
    return _HTTP


class VictoriaMetricsStore:
    name = "victoriametrics"

    def __init__(self, url: str) -> None:
        self._url = url.rstrip("/")
        self._tags = CatalogMem()

    async def ping(self) -> None:
        resp = await _http().get(f"{self._url}/health")
        raise_status(resp, "vm health")

    async def write(self, samples: list[Sample]) -> None:
        if not samples:
            return
        parts = [
            f"prism_sample,tag_id={s.tag_id},quality={s.quality} value={ilp_float(s.value)} {unix_ns(s.ts)}\n"
            for s in samples
        ]
        resp = await _http().post(
            f"{self._url}/write",
            params={"precision": "ns"},
            content="".join(parts).encode("ascii"),
            headers={"Content-Type": "text/plain"},
        )
        raise_status(resp, "vm write")

    async def locf(self, tag_ids: list[int], at: datetime) -> list[Sample]:
        out: list[Sample] = []
        for tag_id in tag_ids:
            resp = await _http().get(
                f"{self._url}/api/v1/query",
                params={"query": f'last_over_time(prism_sample{{tag_id="{tag_id}"}}[30d])', "time": int(at.timestamp())},
            )
            raise_status(resp, "vm query")
            for row in resp.json().get("data", {}).get("result", []):
                ts, raw = row["value"]
                quality = int(row.get("metric", {}).get("quality") or 0)
                out.append(
                    Sample(
                        ts=datetime.fromtimestamp(float(ts), tz=at.tzinfo),
                        tag_id=tag_id,
                        value=float(raw),
                        quality=quality,
                    )
                )
        return out

    async def range(self, tag_ids: list[int], start: datetime, end: datetime) -> list[Sample]:
        seed = await self.locf(tag_ids, start)
        for s in seed:
            s.carried = True
        mid: list[Sample] = []
        for tag_id in tag_ids:
            resp = await _http().get(
                f"{self._url}/api/v1/export",
                params={
                    "match[]": f'prism_sample{{tag_id="{tag_id}"}}',
                    "start": int(start.timestamp()),
                    "end": int(end.timestamp()),
                },
            )
            raise_status(resp, "vm export")
            for line in resp.text.splitlines():
                if not line.strip():
                    continue
                row = json.loads(line)
                quality = int(row.get("metric", {}).get("quality") or 0)
                for ts, val in zip(row.get("timestamps") or [], row.get("values") or []):
                    when = datetime.fromtimestamp(ts / 1000, tz=start.tzinfo)
                    if when <= start or when > end:
                        continue
                    mid.append(Sample(ts=when, tag_id=tag_id, value=float(val), quality=quality))
        return seed + mid

    async def upsert_tags(self, tags: list[Tag]) -> None:
        self._tags.upsert(tags)

    async def list_tags(self) -> list[Tag]:
        return self._tags.list()

    async def close(self) -> None:
        global _HTTP
        if _HTTP is not None and not _HTTP.is_closed:
            await _HTTP.aclose()
        _HTTP = None
