from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

CONTRACT = "v1.1"
OPS = ["write", "locf", "range", "sample", "twavg", "tags"]
QUALITY_GOOD = 192
ReadMode = Literal["locf", "range", "sample", "twavg"]


class Sample(BaseModel):
    ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    tag_id: int
    value: float
    quality: int = QUALITY_GOOD
    carried: bool = False


class WriteRequest(BaseModel):
    samples: list[Sample]


class WriteResponse(BaseModel):
    written: int


class Tag(BaseModel):
    id: int
    name: str
    unit: str = ""


class TagList(BaseModel):
    tags: list[Tag]


class TagWriteRequest(BaseModel):
    tags: list[Tag]


class TagWriteResponse(BaseModel):
    upserted: int


class ReadRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    mode: ReadMode
    tag_ids: list[int]
    at: datetime | None = None
    from_: datetime | None = Field(default=None, alias="from")
    to: datetime | None = None
    step: str = "1m"


class Series(BaseModel):
    tag_id: int
    value: float | None = None
    samples: list[Sample] = Field(default_factory=list)


class ReadResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    mode: ReadMode
    at: datetime | None = None
    from_: datetime | None = Field(default=None, alias="from")
    to: datetime | None = None
    step: str | None = None
    series: list[Series]


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
