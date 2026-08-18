from datetime import datetime, timedelta, timezone

import httpx

from app.models import Point, QueryResult, Sample
from app.store.base import step_seconds


class VictoriaMetricsStore:
    name = "victoriametrics"

    def __init__(self, base: str) -> None:
        self._base = base.rstrip("/")
        self._client = httpx.Client(base_url=self._base, timeout=15.0)

    async def ping(self) -> None:
        resp = self._client.get("/health")
        resp.raise_for_status()

    async def write(self, points: list[Point]) -> None:
        if not points:
            return
        lines: list[str] = []
        for p in points:
            labels = [f'metric="{_esc(p.metric)}"']
            for k, v in (p.labels or {}).items():
                labels.append(f'{_esc(k)}="{_esc(v)}"')
            ms = int(p.ts.timestamp() * 1000)
            lines.append(f'prism_metric{{{",".join(labels)}}} {p.value} {ms}')
        resp = self._client.post("/api/v1/import/prometheus", content="\n".join(lines) + "\n")
        resp.raise_for_status()

    async def query(
        self,
        metric: str,
        start: datetime,
        end: datetime,
        step: timedelta,
        agg: str,
        labels: dict[str, str],
    ) -> QueryResult:
        fn = {"min": "min", "max": "max", "sum": "sum", "count": "count"}.get(agg, "avg")
        resp = self._client.get(
            "/api/v1/query_range",
            params={
                "query": f"{fn}(prism_metric{{{_matchers(metric, labels)}}})",
                "start": int(start.timestamp()),
                "end": int(end.timestamp()),
                "step": f"{step_seconds(step)}s",
            },
        )
        resp.raise_for_status()
        result = resp.json().get("data", {}).get("result", [])
        samples: list[Sample] = []
        if result:
            for ts, raw in result[0].get("values", []):
                samples.append(Sample(ts=datetime.fromtimestamp(float(ts), tz=timezone.utc), value=float(raw)))
        return QueryResult(metric=metric, agg=agg, step=f"{step_seconds(step)}s", points=samples)

    async def latest(self, metric: str, labels: dict[str, str]) -> Point | None:
        resp = self._client.get(
            "/api/v1/query",
            params={"query": f"prism_metric{{{_matchers(metric, labels)}}}"},
        )
        resp.raise_for_status()
        result = resp.json().get("data", {}).get("result", [])
        if not result:
            return None
        sample = result[0]
        ts, raw = sample["value"]
        out_labels = {k: v for k, v in sample.get("metric", {}).items() if k not in {"__name__", "metric"}}
        return Point(ts=datetime.fromtimestamp(float(ts), tz=timezone.utc), metric=metric, value=float(raw), labels=out_labels)

    async def close(self) -> None:
        self._client.close()


def _esc(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "")


def _matchers(metric: str, labels: dict[str, str]) -> str:
    parts = [f'metric="{_esc(metric)}"']
    for k, v in (labels or {}).items():
        parts.append(f'{_esc(k)}="{_esc(v)}"')
    return ",".join(parts)
