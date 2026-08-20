import json
from datetime import datetime, timedelta

from app.models import Sample, Tag
from app.store.catalog import CatalogMem
from app.store.net import ilp_float, new_client, raise_status, unix_ns

_HTTP = None


def _http():
    global _HTTP
    if _HTTP is None or _HTTP.is_closed:
        _HTTP = new_client(timeout=15.0)
    return _HTTP


_LOOKBACK = timedelta(hours=2)  # archive max gap is 1h; 370d last_over_time is overkill


def _tag_re(tag_ids: list[int]) -> str:
    return "|".join(str(i) for i in tag_ids)


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
        seed, _ = await self._scan(tag_ids, at - _LOOKBACK, at, at, at, with_mid=False)
        return seed

    async def range(self, tag_ids: list[int], start: datetime, end: datetime) -> list[Sample]:
        seed, mid = await self._scan(tag_ids, start - _LOOKBACK, end, start, end, with_mid=True)
        return seed + mid

    async def _scan(
        self,
        tag_ids: list[int],
        export_start: datetime,
        export_end: datetime,
        from_: datetime,
        to: datetime,
        with_mid: bool,
    ) -> tuple[list[Sample], list[Sample]]:
        if not tag_ids:
            return [], []
        resp = await _http().get(
            f"{self._url}/api/v1/export",
            params={
                "match[]": f'prism_sample{{tag_id=~"{_tag_re(tag_ids)}"}}',
                "start": int(export_start.timestamp()),
                "end": int(export_end.timestamp()),
            },
        )
        raise_status(resp, "vm export")
        best: dict[int, Sample] = {}
        mid: list[Sample] = []
        for line in resp.text.splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            metric = row.get("metric") or {}
            try:
                tag_id = int(metric.get("tag_id"))
            except (TypeError, ValueError):
                continue
            quality = int(metric.get("quality") or 0)
            for ts, val in zip(row.get("timestamps") or [], row.get("values") or []):
                when = datetime.fromtimestamp(ts / 1000, tz=from_.tzinfo)
                if when <= from_:
                    prev = best.get(tag_id)
                    if prev is None or when > prev.ts:
                        best[tag_id] = Sample(
                            ts=when, tag_id=tag_id, value=float(val), quality=quality, carried=with_mid
                        )
                    continue
                if with_mid and when <= to:
                    mid.append(Sample(ts=when, tag_id=tag_id, value=float(val), quality=quality))
        seed = [best[tag_id] for tag_id in tag_ids if tag_id in best]
        return seed, mid

    async def upsert_tags(self, tags: list[Tag]) -> None:
        self._tags.upsert(tags)

    async def list_tags(self) -> list[Tag]:
        return self._tags.list()

    async def close(self) -> None:
        global _HTTP
        if _HTTP is not None and not _HTTP.is_closed:
            await _HTTP.aclose()
        _HTTP = None
