from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

CONTRACT = "v1.2"
OPS = ["write", "locf", "range", "tags"]
QUALITY_GOOD = 192


@dataclass(slots=True)
class Sample:
    ts: datetime
    tag_id: int
    value: float
    quality: int = QUALITY_GOOD
    carried: bool = False

    def as_carried(self) -> "Sample":
        if self.carried:
            return self
        return replace(self, carried=True)


class WriteItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: int | None = None
    tag_id: int | None = None
    date: datetime | None = None
    ts: datetime | None = None
    value: float
    quality: int | None = QUALITY_GOOD

    def to_sample(self, now: datetime) -> Sample:
        tag = self.id if self.id is not None else self.tag_id
        if tag is None:
            raise ValueError("id is required")
        stamp = self.date or self.ts or now
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        q = QUALITY_GOOD if self.quality is None else self.quality
        return Sample(ts=stamp, tag_id=int(tag), value=self.value, quality=q)


class SamplesWrap(BaseModel):
    samples: list[WriteItem] = Field(default_factory=list)


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


class ValuesRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    request_key: str = Field(default="", alias="requestKey", serialization_alias="requestKey")
    tags_id: list[int] = Field(default_factory=list, alias="tagsId", serialization_alias="tagsId")
    exact: datetime | None = None
    old: datetime | None = None
    young: datetime | None = None

    def mode(self) -> str:
        if self.old is not None and self.young is not None:
            return "range"
        return "locf"

    def at(self) -> datetime:
        if self.exact is not None:
            stamp = self.exact
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            return stamp
        return datetime.now(timezone.utc)


class ValueRecord(BaseModel):
    date: datetime
    value: float
    quality: int


class ValuesTag(BaseModel):
    id: int
    values: list[ValueRecord] = Field(default_factory=list)


class ValuesResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    request_key: str = Field(default="", alias="requestKey", serialization_alias="requestKey")
    tags: list[ValuesTag]


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


def parse_write_payload(payload: Any) -> list[WriteItem]:
    if isinstance(payload, list):
        return [WriteItem.model_validate(item) for item in payload]
    if isinstance(payload, dict):
        if "samples" in payload:
            return SamplesWrap.model_validate(payload).samples
        return [WriteItem.model_validate(payload)]
    raise ValueError("values array is required")


def samples_from_payload(payload: Any, now: datetime) -> list[Sample]:
    if isinstance(payload, dict) and "samples" in payload:
        payload = payload["samples"]
    if not isinstance(payload, list) or not payload:
        raise ValueError("values array is required")
    out: list[Sample] = []
    for raw in payload:
        if not isinstance(raw, dict):
            raise ValueError("values array is required")
        tag = raw.get("id", raw.get("tag_id"))
        if tag is None:
            raise ValueError("id is required")
        stamp = raw.get("date") or raw.get("ts") or now
        if isinstance(stamp, str):
            stamp = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        if isinstance(stamp, datetime) and stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        q = QUALITY_GOOD if raw.get("quality") is None else int(raw["quality"])
        try:
            value = float(raw["value"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("value is required") from exc
        out.append(
            Sample(
                ts=stamp,
                tag_id=int(tag),
                value=value,
                quality=q,
            )
        )
    return out
