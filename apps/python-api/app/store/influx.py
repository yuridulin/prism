from datetime import datetime, timedelta, timezone

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from app.models import Sample, Tag
from app.store.catalog import CatalogMem


class InfluxStore:
    name = "influxdb"

    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        self._client = InfluxDBClient(url=url, token=token, org=org)
        self._org = org
        self._bucket = bucket
        self._write = self._client.write_api(write_options=SYNCHRONOUS)
        self._query = self._client.query_api()
        self._tags = CatalogMem()

    async def ping(self) -> None:
        self._client.ping()

    async def write(self, samples: list[Sample]) -> None:
        if not samples:
            return
        points = [
            Point("samples")
            .tag("tag_id", str(s.tag_id))
            .field("value", float(s.value))
            .field("quality", int(s.quality))
            .time(s.ts)
            for s in samples
        ]
        self._write.write(bucket=self._bucket, org=self._org, record=points)

    async def locf(self, tag_ids: list[int], at: datetime) -> list[Sample]:
        return self._last(tag_ids, at, carried=False)

    async def range(self, tag_ids: list[int], start: datetime, end: datetime) -> list[Sample]:
        seed = self._last(tag_ids, start, carried=True)
        filt = " or ".join(f'r.tag_id == "{i}"' for i in tag_ids) or "true"
        flux = f'''
from(bucket: "{self._bucket}")
  |> range(start: {start.isoformat()}, stop: {(end + timedelta(microseconds=1)).isoformat()})
  |> filter(fn: (r) => r._measurement == "samples")
  |> filter(fn: (r) => {filt})
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
'''
        return seed + self._collect(flux, False)

    def _last(self, tag_ids: list[int], stop: datetime, carried: bool) -> list[Sample]:
        filt = " or ".join(f'r.tag_id == "{i}"' for i in tag_ids) or "true"
        flux = f'''
from(bucket: "{self._bucket}")
  |> range(start: -30d, stop: {stop.isoformat()})
  |> filter(fn: (r) => r._measurement == "samples")
  |> filter(fn: (r) => {filt})
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["tag_id"])
  |> last()
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
        self._client.close()
