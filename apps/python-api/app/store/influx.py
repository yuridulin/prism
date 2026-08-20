import asyncio
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

from influxdb_client import InfluxDBClient

from app.models import Sample, Tag
from app.store.catalog import CatalogMem
from app.store.net import ilp_float, new_client, raise_status, unix_ns


class InfluxStore:
    name = "influxdb"

    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        self._org = org
        self._bucket = bucket
        self._write_path = (
            f"api/v2/write?org={quote(org)}&bucket={quote(bucket)}&precision=ns"
        )
        self._http = new_client(
            timeout=30.0,
            base_url=url.rstrip("/") + "/",
            headers={"Authorization": f"Token {token}"},
        )
        self._client = InfluxDBClient(url=url, token=token, org=org)
        self._query = self._client.query_api()
        self._tags = CatalogMem()

    async def ping(self) -> None:
        resp = await self._http.get("health")
        if resp.status_code >= 300:
            raise RuntimeError("influxdb ping failed")

    async def write(self, samples: list[Sample]) -> None:
        if not samples:
            return
        parts = [
            f"samples,tag_id={s.tag_id} value={ilp_float(s.value)},quality={s.quality}i {unix_ns(s.ts)}\n"
            for s in samples
        ]
        resp = await self._http.post(
            self._write_path,
            content="".join(parts).encode("ascii"),
            headers={"Content-Type": "text/plain; charset=utf-8"},
        )
        raise_status(resp, "influxdb write")

    async def locf(self, tag_ids: list[int], at: datetime) -> list[Sample]:
        return await asyncio.to_thread(self._last, tag_ids, at, False)

    async def range(self, tag_ids: list[int], start: datetime, end: datetime) -> list[Sample]:
        seed = await asyncio.to_thread(self._last, tag_ids, start, True)
        filt = " or ".join(f'r.tag_id == "{i}"' for i in tag_ids) or "true"
        flux = f'''
from(bucket: "{self._bucket}")
  |> range(start: {(start + timedelta(microseconds=1)).isoformat()}, stop: {(end + timedelta(microseconds=1)).isoformat()})
  |> filter(fn: (r) => r._measurement == "samples")
  |> filter(fn: (r) => {filt})
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
'''
        return seed + await asyncio.to_thread(self._collect, flux, False)

    def _last(self, tag_ids: list[int], stop: datetime, carried: bool) -> list[Sample]:
        filt = " or ".join(f'r.tag_id == "{i}"' for i in tag_ids) or "true"
        # Archive max gap is 1h; 3h still finds the previous minute/hour point at 364d ago.
        start = stop - timedelta(hours=3)
        flux = f'''
from(bucket: "{self._bucket}")
  |> range(start: {start.isoformat()}, stop: {(stop + timedelta(microseconds=1)).isoformat()})
  |> filter(fn: (r) => r._measurement == "samples")
  |> filter(fn: (r) => {filt})
  |> last()
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
'''
        return self._collect(flux, carried)

    def _collect(self, flux: str, carried: bool) -> list[Sample]:
        tables = self._query.query(flux, org=self._org)
        out: list[Sample] = []
        for table in tables:
            for rec in table.records:
                out.append(
                    Sample(
                        ts=rec.get_time() or datetime.now(timezone.utc),
                        tag_id=int(rec.values.get("tag_id") or 0),
                        value=float(rec.values.get("value") or 0),
                        quality=int(rec.values.get("quality") or 0),
                        carried=carried,
                    )
                )
        return out

    async def upsert_tags(self, tags: list[Tag]) -> None:
        self._tags.upsert(tags)

    async def list_tags(self) -> list[Tag]:
        return self._tags.list()

    async def close(self) -> None:
        await self._http.aclose()
        self._client.close()
