from datetime import datetime, timezone
from urllib.parse import quote
import csv
import io

from app.models import Sample, Tag
from app.store.catalog import CatalogMem
from app.store.net import ilp_float, new_client, raise_status, unix_ns


def _influxql_time(ts: datetime) -> str:
    utc = ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return "'" + utc.isoformat().replace("+00:00", "Z") + "'"


def _tag_id_from_tags(raw: str) -> int:
    for part in raw.strip('"').split(","):
        if "=" in part:
            key, val = part.split("=", 1)
            if key.strip() == "tag_id":
                return int(val)
    return 0


def _parse_influx_csv(text: str, carried: bool) -> list[Sample]:
    if not text:
        return []
    reader = csv.reader(io.StringIO(text))
    idx: dict[str, int] | None = None
    out: list[Sample] = []
    for row in reader:
        if not row or row[0].startswith("#"):
            continue
        if idx is None:
            candidate = {name.strip().lower(): i for i, name in enumerate(row)}
            if "time" not in candidate:
                continue
            idx = candidate
            continue
        ti, vi = idx.get("time"), idx.get("value")
        if ti is None or vi is None or ti >= len(row) or vi >= len(row):
            continue
        if "tag_id" in idx and idx["tag_id"] < len(row) and row[idx["tag_id"]]:
            tag_id = int(float(row[idx["tag_id"]]))
        elif "tags" in idx and idx["tags"] < len(row):
            tag_id = _tag_id_from_tags(row[idx["tags"]])
        else:
            tag_id = 0
        quality = 0
        if "quality" in idx and idx["quality"] < len(row) and row[idx["quality"]]:
            quality = int(float(row[idx["quality"]]))
        ts_raw = row[ti]
        try:
            ms = int(float(ts_raw))
            ts = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        except ValueError:
            ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        out.append(
            Sample.model_construct(
                ts=ts, tag_id=tag_id, value=float(row[vi] or 0), quality=quality, carried=carried
            )
        )
    return out


def _tag_re(tag_ids: list[int]) -> str:
    if not tag_ids:
        return "true"
    return "tag_id =~ /^(" + "|".join(str(i) for i in tag_ids) + ")$/"


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
        self._tags = CatalogMem()

    async def ping(self) -> None:
        resp = await self._http.get("health")
        if resp.status_code >= 300:
            raise RuntimeError("influxdb ping failed")
        await self._ensure_dbrp()

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
        return await self._last(tag_ids, at, False)

    async def range(self, tag_ids: list[int], start: datetime, end: datetime) -> list[Sample]:
        seed = await self._last(tag_ids, start, True)
        q = (
            f'SELECT "value", "quality" FROM "samples" '
            f"WHERE time > {_influxql_time(start)} AND time <= {_influxql_time(end)} AND {_tag_re(tag_ids)}"
        )
        return seed + await self._query(q, False)

    async def _last(self, tag_ids: list[int], stop: datetime, carried: bool) -> list[Sample]:
        q = (
            f'SELECT last("value") AS "value", last("quality") AS "quality" FROM "samples" '
            f"WHERE time <= {_influxql_time(stop)} AND {_tag_re(tag_ids)} "
            f'GROUP BY "tag_id"'
        )
        return await self._query(q, carried)

    async def _query(self, q: str, carried: bool) -> list[Sample]:
        resp = await self._http.post(
            "query",
            data={"org": self._org, "bucket": self._bucket, "db": self._bucket, "epoch": "ms", "q": q},
            headers={"Accept": "application/csv"},
        )
        raise_status(resp, "influxdb query")
        text = resp.text
        if not text or text[:1] != "{":
            return _parse_influx_csv(text, carried)
        payload = resp.json()
        out: list[Sample] = []
        for result in payload.get("results") or []:
            if result.get("error"):
                raise RuntimeError(result["error"])
            for series in result.get("series") or []:
                tag_id = int((series.get("tags") or {}).get("tag_id") or 0)
                cols = {name: i for i, name in enumerate(series.get("columns") or [])}
                ti, vi = cols.get("time"), cols.get("value")
                qi = cols.get("quality")
                if ti is None or vi is None:
                    continue
                for row in series.get("values") or []:
                    if ti >= len(row) or vi >= len(row):
                        continue
                    ts = datetime.fromtimestamp(int(row[ti]) / 1000, tz=timezone.utc)
                    quality = int(float(row[qi])) if qi is not None and qi < len(row) else 0
                    out.append(
                        Sample.model_construct(
                            ts=ts,
                            tag_id=tag_id,
                            value=float(row[vi]),
                            quality=quality,
                            carried=carried,
                        )
                    )
        return out

    async def _ensure_dbrp(self) -> None:
        listed = await self._http.get("api/v2/dbrps", params={"org": self._org, "db": self._bucket})
        if listed.status_code < 300:
            content = (listed.json() or {}).get("content") or []
            if content:
                return
        buckets = await self._http.get("api/v2/buckets", params={"org": self._org, "name": self._bucket})
        raise_status(buckets, "influx buckets")
        items = (buckets.json() or {}).get("buckets") or []
        if not items:
            raise RuntimeError(f"influx bucket {self._bucket} not found")
        resp = await self._http.post(
            "api/v2/dbrps",
            params={"org": self._org},
            json={
                "org": self._org,
                "bucketID": items[0]["id"],
                "database": self._bucket,
                "retention_policy": "autogen",
                "default": True,
            },
        )
        if resp.status_code >= 300 and resp.status_code != 409:
            raise_status(resp, "influx dbrp")

    async def upsert_tags(self, tags: list[Tag]) -> None:
        self._tags.upsert(tags)

    async def list_tags(self) -> list[Tag]:
        return self._tags.list()

    async def close(self) -> None:
        await self._http.aclose()
