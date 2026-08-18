from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field

Agg = Literal["avg", "min", "max", "sum", "count"]


class Point(BaseModel):
    ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metric: str
    value: float
    labels: dict[str, str] = Field(default_factory=dict)


class WriteRequest(BaseModel):
    points: list[Point]


class Sample(BaseModel):
    ts: datetime
    value: float


class QueryResult(BaseModel):
    metric: str
    agg: str
    step: str
    points: list[Sample]


class Meta(BaseModel):
    backend: str
    storage: str
    storages: list[str]
