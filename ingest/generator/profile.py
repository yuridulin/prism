"""Load and validate Prism load profiles (profiles/*.yaml)."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from pathlib import Path
import os

import yaml


class ProfileError(ValueError):
    pass


@dataclass(frozen=True)
class LabelDim:
    name: str
    values: tuple[str, ...]


@dataclass(frozen=True)
class QueryMixItem:
    op: str
    weight: float
    window: str = "15m"
    step: str = "1m"
    agg: str = "avg"


@dataclass(frozen=True)
class IngestSpec:
    enabled: bool = True
    rate: float = 1000
    batch: int = 100
    workers: int = 1
    metrics: tuple[str, ...] = ("cpu.usage",)
    labels: tuple[LabelDim, ...] = ()
    out_of_order: float = 0.0
    late_ms: int = 0


@dataclass(frozen=True)
class QuerySpec:
    enabled: bool = False
    rate: float = 0
    mix: tuple[QueryMixItem, ...] = ()


@dataclass(frozen=True)
class Profile:
    name: str
    description: str
    transport: str
    duration: float
    ingest: IngestSpec
    query: QuerySpec

    def label_dims(self) -> tuple[LabelDim, ...]:
        return self.ingest.labels

    def sample_space_size(self) -> int:
        size = max(len(self.ingest.metrics), 1)
        for dim in self.ingest.labels:
            size *= max(len(dim.values), 1)
        return size


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
    if not ingest.enabled and not query.enabled:
        raise ProfileError("profile must enable ingest and/or query")
    return Profile(
        name=name,
        description=str(raw.get("description") or ""),
        transport=transport,
        duration=float(raw.get("duration") or 0),
        ingest=ingest,
        query=query,
    )


def _parse_ingest(raw: dict) -> IngestSpec:
    metrics = tuple(str(m) for m in (raw.get("metrics") or ["cpu.usage"]))
    if not metrics:
        raise ProfileError("ingest.metrics must not be empty")
    return IngestSpec(
        enabled=bool(raw.get("enabled", True)),
        rate=float(raw.get("rate") or 0),
        batch=max(int(raw.get("batch") or 1), 1),
        workers=max(int(raw.get("workers") or 1), 1),
        metrics=metrics,
        labels=_parse_labels(raw.get("labels") or {}),
        out_of_order=min(max(float(raw.get("out_of_order") or 0), 0.0), 1.0),
        late_ms=max(int(raw.get("late_ms") or 0), 0),
    )


def _parse_query(raw: dict) -> QuerySpec:
    mix = []
    for item in raw.get("mix") or []:
        op = str(item.get("op") or "").lower()
        if op not in {"query", "latest"}:
            raise ProfileError("query.mix.op must be query or latest")
        mix.append(
            QueryMixItem(
                op=op,
                weight=max(float(item.get("weight") or 1), 0.0),
                window=str(item.get("window") or "15m"),
                step=str(item.get("step") or "1m"),
                agg=str(item.get("agg") or "avg"),
            )
        )
    enabled = bool(raw.get("enabled", False))
    if enabled and not mix:
        raise ProfileError("query.mix is required when query.enabled is true")
    return QuerySpec(enabled=enabled, rate=float(raw.get("rate") or 0), mix=tuple(mix))


def _parse_labels(raw: dict) -> tuple[LabelDim, ...]:
    dims: list[LabelDim] = []
    for name, spec in raw.items():
        if not isinstance(spec, dict):
            raise ProfileError(f"labels.{name} must be a mapping")
        if spec.get("values"):
            values = tuple(str(v) for v in spec["values"])
        else:
            count = int(spec.get("count") or 0)
            if count <= 0:
                raise ProfileError(f"labels.{name} needs values or count")
            prefix = str(spec.get("prefix") or f"{name}-")
            width = int(spec.get("width") or len(str(max(count - 1, 0))))
            values = tuple(f"{prefix}{i:0{width}d}" for i in range(count))
        dims.append(LabelDim(name=str(name), values=values))
    return tuple(dims)


def random_labels(dims: tuple[LabelDim, ...], rng) -> dict[str, str]:
    return {dim.name: rng.choice(dim.values) for dim in dims}


def enumerate_labels(dims: tuple[LabelDim, ...], limit: int = 10_000) -> list[dict[str, str]]:
    if not dims:
        return [{}]
    keys = [d.name for d in dims]
    out: list[dict[str, str]] = []
    for combo in product(*(d.values for d in dims)):
        out.append(dict(zip(keys, combo, strict=True)))
        if len(out) >= limit:
            break
    return out


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


def to_k6_env(profile: Profile) -> dict:
    """Minimal JSON-serializable view for k6."""
    return {
        "name": profile.name,
        "transport": profile.transport,
        "duration": profile.duration,
        "ingest": {
            "enabled": profile.ingest.enabled,
            "rate": profile.ingest.rate,
            "batch": profile.ingest.batch,
            "metrics": list(profile.ingest.metrics),
            "labels": {d.name: list(d.values[:50]) for d in profile.ingest.labels},
        },
        "query": {
            "enabled": profile.query.enabled,
            "rate": profile.query.rate,
            "mix": [
                {
                    "op": i.op,
                    "weight": i.weight,
                    "window": i.window,
                    "step": i.step,
                    "agg": i.agg,
                }
                for i in profile.query.mix
            ],
        },
    }
