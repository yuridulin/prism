from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

Agg = Literal["avg", "min", "max", "sum", "count"]
CONTRACT = "v1"
OPS = ["write", "query", "latest"]


class Point(BaseModel):
    ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metric: str
    value: float
    labels: dict[str, str] = Field(default_factory=dict)


class WriteRequest(BaseModel):
    points: list[Point]


class WriteResponse(BaseModel):
    written: int


class QueryRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    metric: str
    from_: datetime = Field(alias="from")
    to: datetime
    step: str = "1m"
    agg: Agg = "avg"
    labels: dict[str, str] = Field(default_factory=dict)


class LatestRequest(BaseModel):
    metric: str
    labels: dict[str, str] = Field(default_factory=dict)


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
    contract: str = CONTRACT
    ops: list[str] = Field(default_factory=lambda: list(OPS))


class ErrorDetail(BaseModel):
    code: str
    message: str


class ErrorBody(BaseModel):
    error: ErrorDetail
