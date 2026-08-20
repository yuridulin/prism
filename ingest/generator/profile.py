"""Load and validate Prism load profiles (profiles/*.yaml)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import os

import yaml


class ProfileError(ValueError):
    pass


_DURATION_UNITS = (("ms", 0.001), ("d", 86400), ("h", 3600), ("m", 60), ("s", 1))


def parse_duration(raw: str | int | float) -> timedelta:
    if isinstance(raw, (int, float)):
        return timedelta(seconds=float(raw))
    text = str(raw).strip()
    if text.replace(".", "", 1).isdigit():
        return timedelta(seconds=float(text))
    for suffix, mul in _DURATION_UNITS:
        if text.endswith(suffix) and text[: -len(suffix)]:
            return timedelta(seconds=float(text[: -len(suffix)]) * mul)
    raise ProfileError(f"invalid duration {raw!r}")


def rfc3339(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class QueryMixItem:
    op: str
    weight: float
    window: str = "15m"
    step: str = "1m"


@dataclass(frozen=True)
class IngestSpec:
    enabled: bool = True
    rate: float = 1000
    batch: int = 100
    workers: int = 1
    tag_start: int = 1
    tag_count: int = 100
    good_ratio: float = 0.98
    out_of_order: float = 0.0
    late_ms: int = 0


@dataclass(frozen=True)
class TagClass:
    name: str
    start: int
    count: int
    period: str

    def ids(self) -> list[int]:
        return list(range(self.start, self.start + self.count))


@dataclass(frozen=True)
class ArchiveSpec:
    enabled: bool = False
    span: str = "365d"
    batch: int = 2000
    workers: int = 4
    tags: tuple[TagClass, ...] = ()

    def class_named(self, name: str) -> TagClass:
        for item in self.tags:
            if item.name == name:
                return item
        raise ProfileError(f"archive has no tag class {name!r}")

    def all_ids(self) -> list[int]:
        ids: list[int] = []
        for item in self.tags:
            ids.extend(item.ids())
        return ids


@dataclass(frozen=True)
class TagSetSpec:
    name: str
    size: int


@dataclass(frozen=True)
class QuerySpec:
    enabled: bool = False
    rate: float = 0
    workers: int = 1
    rng_seed: int = 42
    mix: tuple[QueryMixItem, ...] = ()
    tag_sets: tuple[TagSetSpec, ...] = ()
    at_offsets: tuple[str, ...] = ()
    end_offsets: tuple[str, ...] = ()


@dataclass(frozen=True)
class QueryCall:
    op: str
    tag_ids: tuple[int, ...]
    at: datetime | None = None
    start: datetime | None = None
    end: datetime | None = None
    step: str | None = None
    label: str = ""

    def payload(self) -> dict:
        body: dict = {"mode": self.op, "tag_ids": list(self.tag_ids)}
        if self.op == "locf":
            assert self.at is not None
            body["at"] = rfc3339(self.at)
            return body
        assert self.start is not None and self.end is not None
        body["from"] = rfc3339(self.start)
        body["to"] = rfc3339(self.end)
        if self.op == "sample":
            body["step"] = self.step or "1m"
        return body


@dataclass(frozen=True)
class Profile:
    name: str
    description: str
    transport: str
    duration: float
    ingest: IngestSpec
    query: QuerySpec
    archive: ArchiveSpec

    def sample_space_size(self) -> int:
        if self.archive.enabled and self.archive.tags:
            return len(self.archive.all_ids())
        return max(self.ingest.tag_count, 1)


def profiles_dir(explicit: str | None = None) -> Path:
    if explicit:
        return Path(explicit)
    env = os.getenv("PROFILES_DIR")
    if env:
        return Path(env)
    return Path(__file__).resolve().parents[2] / "profiles"


def _as_dir(root: Path | str | None = None) -> Path:
    if root is None:
        return profiles_dir()
    return Path(root)


def list_profiles(root: Path | str | None = None) -> list[str]:
    directory = _as_dir(root)
    return sorted(p.stem for p in directory.glob("*.yaml") if not p.name.startswith("_"))


def load_profile(name: str, root: Path | str | None = None) -> Profile:
    directory = _as_dir(root)
    path = Path(name)
    if path.suffix in {".yaml", ".yml"} and path.exists():
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    else:
        candidate = directory / f"{name}.yaml"
        if not candidate.exists():
            available = ", ".join(list_profiles(directory)) or "(none)"
            raise ProfileError(f"profile {name!r} not found in {directory}. available: {available}")
        raw = yaml.safe_load(candidate.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ProfileError("profile must be a mapping")
    return _parse(raw)


def _parse(raw: dict) -> Profile:
    name = str(raw.get("name") or "").strip()
    if not name:
        raise ProfileError("profile.name is required")
    transport = str(raw.get("transport") or "nats").lower()
    if transport not in {"nats", "http"}:
        raise ProfileError("transport must be nats or http")
    ingest = _parse_ingest(raw.get("ingest") or {})
    query = _parse_query(raw.get("query") or {})
    archive = _parse_archive(raw.get("archive") or {})
    if not ingest.enabled and not query.enabled and not archive.enabled:
        raise ProfileError("profile must enable ingest, archive and/or query")
    return Profile(
        name=name,
        description=str(raw.get("description") or ""),
        transport=transport,
        duration=float(raw.get("duration") or 0),
        ingest=ingest,
        query=query,
        archive=archive,
    )


def _parse_ingest(raw: dict) -> IngestSpec:
    return IngestSpec(
        enabled=bool(raw.get("enabled", True)),
        rate=float(raw.get("rate") or 0),
        batch=max(int(raw.get("batch") or 1), 1),
        workers=max(int(raw.get("workers") or 1), 1),
        tag_start=max(int(raw.get("tag_start") or 1), 0),
        tag_count=max(int(raw.get("tag_count") or 1), 1),
        good_ratio=min(max(float(raw.get("good_ratio") or 0.98), 0.0), 1.0),
        out_of_order=min(max(float(raw.get("out_of_order") or 0), 0.0), 1.0),
        late_ms=max(int(raw.get("late_ms") or 0), 0),
    )


def _parse_archive(raw: dict) -> ArchiveSpec:
    tags = []
    seen: set[int] = set()
    for item in raw.get("tags") or []:
        name = str(item.get("class") or item.get("name") or "").strip()
        if not name:
            raise ProfileError("archive.tags.class is required")
        start = max(int(item.get("start") or item.get("tag_start") or 1), 0)
        count = max(int(item.get("count") or 1), 1)
        period = str(item.get("period") or "1h")
        parse_duration(period)
        ids = range(start, start + count)
        overlap = seen.intersection(ids)
        if overlap:
            raise ProfileError(f"archive tag id {sorted(overlap)[0]} is in more than one class")
        seen.update(ids)
        tags.append(TagClass(name=name, start=start, count=count, period=period))
    enabled = bool(raw.get("enabled", False))
    if enabled and not tags:
        raise ProfileError("archive.tags is required when archive.enabled is true")
    return ArchiveSpec(
        enabled=enabled,
        span=str(raw.get("span") or "365d"),
        batch=max(int(raw.get("batch") or 2000), 1),
        workers=max(int(raw.get("workers") or 1), 1),
        tags=tuple(tags),
    )


def _parse_query(raw: dict) -> QuerySpec:
    mix = []
    aliases = {"query": "range", "latest": "locf", "exact": "locf"}
    for item in raw.get("mix") or []:
        op = aliases.get(str(item.get("op") or "").lower(), str(item.get("op") or "").lower())
        if op not in {"locf", "range", "sample", "twavg"}:
            raise ProfileError("query.mix.op must be locf, range, sample or twavg")
        window = str(item.get("window") or "15m")
        if op != "locf":
            parse_duration(window)
        mix.append(
            QueryMixItem(
                op=op,
                weight=max(float(item.get("weight") or 1), 0.0),
                window=window,
                step=str(item.get("step") or "1m"),
            )
        )
    tag_sets = []
    for item in raw.get("tag_sets") or []:
        name = str(item.get("class") or item.get("name") or "").strip()
        if not name:
            raise ProfileError("query.tag_sets.class is required")
        tag_sets.append(TagSetSpec(name=name, size=max(int(item.get("size") or 1), 1)))
    at_offsets = tuple(str(v) for v in (raw.get("at_offsets") or []))
    end_offsets = tuple(str(v) for v in (raw.get("end_offsets") or []))
    for offset in (*at_offsets, *end_offsets):
        parse_duration(offset)
    enabled = bool(raw.get("enabled", False))
    if enabled and not mix:
        raise ProfileError("query.mix is required when query.enabled is true")
    return QuerySpec(
        enabled=enabled,
        rate=float(raw.get("rate") or 0),
        workers=max(int(raw.get("workers") or 1), 1),
        rng_seed=int(raw.get("rng_seed") or 42),
        mix=tuple(mix),
        tag_sets=tuple(tag_sets),
        at_offsets=at_offsets,
        end_offsets=end_offsets,
    )


def pick_mix(mix: tuple[QueryMixItem, ...], rng) -> QueryMixItem:
    total = sum(item.weight for item in mix)
    if total <= 0:
        return mix[0]
    cursor = rng.random() * total
    acc = 0.0
    for item in mix:
        acc += item.weight
        if cursor <= acc:
            return item
    return mix[-1]


def resolve_tag_set(archive: ArchiveSpec, spec: TagSetSpec) -> list[int]:
    if spec.name == "mixed":
        if len(archive.tags) < 2:
            raise ProfileError("query tag set 'mixed' needs at least two archive classes")
        per = max(spec.size // len(archive.tags), 1)
        ids: list[int] = []
        for cls in archive.tags:
            ids.extend(cls.ids()[:per])
        return ids[: spec.size]
    return archive.class_named(spec.name).ids()[: spec.size]


def default_tag_sets(archive: ArchiveSpec) -> tuple[TagSetSpec, ...]:
    sets = []
    for cls in archive.tags:
        sets.append(TagSetSpec(name=cls.name, size=1))
        if cls.count >= 8:
            sets.append(TagSetSpec(name=cls.name, size=min(8, cls.count)))
    if len(archive.tags) >= 2:
        sets.append(TagSetSpec(name="mixed", size=8))
    return tuple(sets)


def archive_bounds(span: str, end: datetime) -> tuple[datetime, datetime]:
    end = end.astimezone(timezone.utc).replace(second=0, microsecond=0)
    start = end - parse_duration(span)
    return start, end


def archive_sample_count(archive: ArchiveSpec, start: datetime, end: datetime) -> int:
    total = 0
    span = (end - start).total_seconds()
    for cls in archive.tags:
        period = parse_duration(cls.period).total_seconds()
        ticks = int(span / period) + 1
        total += ticks * cls.count
    return total


def build_query_grid(profile: Profile, archive_start: datetime, archive_end: datetime) -> list[QueryCall]:
    spec = profile.query
    tag_sets = spec.tag_sets or default_tag_sets(profile.archive)
    if not tag_sets:
        raise ProfileError("query.tag_sets is required for an archive read grid")
    resolved = [(item, tuple(resolve_tag_set(profile.archive, item))) for item in tag_sets]
    at_offsets = spec.at_offsets or ("0s",)
    end_offsets = spec.end_offsets or ("0s",)
    calls: list[QueryCall] = []
    for mix in spec.mix:
        copies = max(int(round(mix.weight)), 1)
        built: list[QueryCall] = []
        if mix.op == "locf":
            for offset in at_offsets:
                at = archive_end - parse_duration(offset)
                if at < archive_start:
                    at = archive_start
                for item, ids in resolved:
                    built.append(
                        QueryCall(
                            op="locf",
                            tag_ids=ids,
                            at=at,
                            label=f"locf@{offset}/{item.name}-{item.size}",
                        )
                    )
        else:
            window = parse_duration(mix.window)
            for offset in end_offsets:
                qend = archive_end - parse_duration(offset)
                qstart = qend - window
                if qstart < archive_start or qend > archive_end:
                    continue
                for item, ids in resolved:
                    built.append(
                        QueryCall(
                            op=mix.op,
                            tag_ids=ids,
                            start=qstart,
                            end=qend,
                            step=mix.step,
                            label=f"{mix.op} {mix.window}@{offset}/{item.name}-{item.size}",
                        )
                    )
        if not built:
            continue
        for _ in range(copies):
            calls.extend(built)
    if not calls:
        raise ProfileError("query grid is empty: windows do not fit the archive span")
    return calls


def to_k6_env(profile: Profile) -> dict:
    return {
        "name": profile.name,
        "transport": profile.transport,
        "duration": profile.duration,
        "ingest": {
            "enabled": profile.ingest.enabled,
            "rate": profile.ingest.rate,
            "batch": profile.ingest.batch,
            "tag_start": profile.ingest.tag_start,
            "tag_count": profile.ingest.tag_count,
            "good_ratio": profile.ingest.good_ratio,
        },
        "archive": {
            "enabled": profile.archive.enabled,
            "span": profile.archive.span,
            "batch": profile.archive.batch,
            "workers": profile.archive.workers,
            "tags": [
                {"class": t.name, "start": t.start, "count": t.count, "period": t.period}
                for t in profile.archive.tags
            ],
        },
        "query": {
            "enabled": profile.query.enabled,
            "rate": profile.query.rate,
            "workers": profile.query.workers,
            "rng_seed": profile.query.rng_seed,
            "mix": [{"op": i.op, "weight": i.weight, "window": i.window, "step": i.step} for i in profile.query.mix],
            "tag_sets": [{"class": i.name, "size": i.size} for i in profile.query.tag_sets],
            "at_offsets": list(profile.query.at_offsets),
            "end_offsets": list(profile.query.end_offsets),
        },
    }
