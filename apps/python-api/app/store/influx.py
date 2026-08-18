from datetime import datetime, timedelta

from influxdb_client import InfluxDBClient, Point as InfluxPoint, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from app.models import Point, QueryResult, Sample
from app.store.base import step_seconds


class InfluxStore:
    name = "influxdb"

    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        self._client = InfluxDBClient(url=url, token=token, org=org)
        self._org = org
        self._bucket = bucket
        self._write = self._client.write_api(write_options=SYNCHRONOUS)
        self._query = self._client.query_api()

    async def ping(self) -> None:
        if not self._client.ping():
            raise RuntimeError("influxdb ping failed")

    async def write(self, points: list[Point]) -> None:
        if not points:
            return
        batch = []
        for p in points:
            pt = InfluxPoint("prism").time(p.ts, WritePrecision.NS).field("value", float(p.value)).tag("metric", p.metric)
            for k, v in (p.labels or {}).items():
                pt = pt.tag(k, v)
            batch.append(pt)
        self._write.write(bucket=self._bucket, org=self._org, record=batch)

    async def query(
        self,
        metric: str,
        start: datetime,
        end: datetime,
        step: timedelta,
        agg: str,
        labels: dict[str, str],
    ) -> QueryResult:
        fn = {"min": "min", "max": "max", "sum": "sum", "count": "count"}.get(agg, "mean")
        filters = ""
        for k, v in (labels or {}).items():
            filters += f'  |> filter(fn: (r) => r["{k}"] == "{v}")\n'
        flux = f'''
from(bucket: "{self._bucket}")
  |> range(start: {start.isoformat()}, stop: {end.isoformat()})
  |> filter(fn: (r) => r._measurement == "prism" and r.metric == "{metric}" and r._field == "value")
{filters}
  |> aggregateWindow(every: {step_seconds(step)}s, fn: {fn}, createEmpty: false)
  |> keep(columns: ["_time", "_value"])
'''
        tables = self._query.query(flux, org=self._org)
        samples: list[Sample] = []
        for table in tables:
            for rec in table.records:
                samples.append(Sample(ts=rec.get_time(), value=float(rec.get_value())))
        return QueryResult(metric=metric, agg=agg, step=f"{step_seconds(step)}s", points=samples)

    async def latest(self, metric: str, labels: dict[str, str]) -> Point | None:
        filters = ""
        for k, v in (labels or {}).items():
            filters += f'  |> filter(fn: (r) => r["{k}"] == "{v}")\n'
        flux = f'''
from(bucket: "{self._bucket}")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "prism" and r.metric == "{metric}" and r._field == "value")
{filters}
  |> last()
'''
        tables = self._query.query(flux, org=self._org)
        for table in tables:
            for rec in table.records:
                out_labels = {
                    k: str(v)
                    for k, v in rec.values.items()
                    if k not in {"_measurement", "_field", "_time", "_value", "_start", "_stop", "result", "table", "metric"}
                    and isinstance(v, str)
                }
                return Point(ts=rec.get_time(), metric=metric, value=float(rec.get_value()), labels=out_labels)
        return None

    async def close(self) -> None:
        self._client.close()
