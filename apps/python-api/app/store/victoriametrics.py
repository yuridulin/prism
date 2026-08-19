from datetime import datetime

import httpx

from app.models import Sample, Tag
from app.store.catalog import CatalogMem


class VictoriaMetricsStore:
    name = "victoriametrics"

    def __init__(self, url: str) -> None:
        self._url = url.rstrip("/")
        self._http = httpx.Client(timeout=15.0)
        self._tags = CatalogMem()

    async def ping(self) -> None:
        resp = self._http.get(f"{self._url}/health")
        resp.raise_for_status()

    async def write(self, samples: list[Sample]) -> None:
        if not samples:
            return
        lines = [
            f'prism_sample{{tag_id="{s.tag_id}",quality="{s.quality}"}} {s.value} {int(s.ts.timestamp() * 1000)}'
            for s in samples
        ]
        resp = self._http.post(f"{self._url}/api/v1/import/prometheus", content="\n".join(lines) + "\n")
        resp.raise_for_status()

    async def locf(self, tag_ids: list[int], at: datetime) -> list[Sample]:
        out: list[Sample] = []
        for tag_id in tag_ids:
            resp = self._http.get(
                f"{self._url}/api/v1/query",
                params={"query": f'last_over_time(prism_sample{{tag_id="{tag_id}"}}[30d])', "time": int(at.timestamp())},
            )
            resp.raise_for_status()
            for row in resp.json().get("data", {}).get("result", []):
                ts, raw = row["value"]
                quality = int(row.get("metric", {}).get("quality") or 0)
                out.append(Sample(ts=datetime.fromtimestamp(float(ts), tz=at.tzinfo), tag_id=tag_id, value=float(raw), quality=quality))
        return out

    async def range(self, tag_ids: list[int], start: datetime, end: datetime) -> list[Sample]:
        seed = await self.locf(tag_ids, start)
        for s in seed:
            s.carried = True
        mid: list[Sample] = []
        for tag_id in tag_ids:
            resp = self._http.get(
                f"{self._url}/api/v1/export",
                params={
                    "match[]": f'prism_sample{{tag_id="{tag_id}"}}',
                    "start": int(start.timestamp()),
                    "end": int(end.timestamp()),
                },
            )
            resp.raise_for_status()
            for line in resp.text.splitlines():
                if not line.strip():
                    continue
                import json

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
        self._http.close()
