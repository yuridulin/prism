from datetime import datetime, timedelta
from typing import Protocol

from app.models import Point, QueryResult


class Store(Protocol):
    name: str

    async def ping(self) -> None: ...
    async def write(self, points: list[Point]) -> None: ...
    async def query(
        self,
        metric: str,
        start: datetime,
        end: datetime,
        step: timedelta,
        agg: str,
        labels: dict[str, str],
    ) -> QueryResult: ...
    async def latest(self, metric: str, labels: dict[str, str]) -> Point | None: ...
    async def close(self) -> None: ...


def agg_sql(agg: str) -> str:
    return {"min": "min", "max": "max", "sum": "sum", "count": "count"}.get(agg, "avg")


def step_seconds(step: timedelta) -> int:
    return max(int(step.total_seconds()), 1)
