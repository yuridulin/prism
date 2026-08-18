"""Session records: when / what / why / results / conclusions."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import re

import yaml

ROOT = Path(__file__).resolve().parents[1]
SESSIONS_DIR = Path(__file__).resolve().parent
DEFAULTS_PATH = SESSIONS_DIR / "defaults.yaml"
CATALOG_PATH = SESSIONS_DIR / "catalog.yaml"

RESOURCE_GROUPS = ("api", "storage", "bus", "observe", "generator")
VALID_BACKENDS = ("go", "python")
VALID_STORAGES = ("timescaledb", "clickhouse", "influxdb", "victoriametrics")
API_SERVICE = {"go": "go-api", "python": "python-api"}
API_URL = {"go": "http://go-api:8081", "python": "http://python-api:8082"}
API_READY = {"go": "http://127.0.0.1:8081/readyz", "python": "http://127.0.0.1:8082/readyz"}
API_META = {"go": "http://127.0.0.1:8081/v1/meta", "python": "http://127.0.0.1:8082/v1/meta"}


class SessionError(ValueError):
    pass


def utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_duration(raw: str | int | float) -> int:
    if isinstance(raw, (int, float)):
        seconds = int(raw)
        if seconds <= 0:
            raise SessionError("duration must be positive")
        return seconds
    text = str(raw).strip()
    if text.isdigit():
        return parse_duration(int(text))
    match = re.fullmatch(r"(\d+(?:\.\d+)?)(ms|s|m|h)", text)
    if not match:
        raise SessionError(f"invalid duration {raw!r}, expected 5m / 300s / 300")
    amount = float(match.group(1))
    mul = {"ms": 0.001, "s": 1, "m": 60, "h": 3600}[match.group(2)]
    seconds = int(amount * mul)
    if seconds <= 0:
        raise SessionError("duration must be positive")
    return seconds


def format_duration(seconds: int) -> str:
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def load_yaml(path: Path) -> dict:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SessionError(f"{path} must be a mapping")
    return raw


def dump_yaml(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")


def load_defaults() -> dict:
    return load_yaml(DEFAULTS_PATH)


def pair_slug(pair: dict) -> str:
    return f"{pair['backend']}-{pair['storage']}"


def normalize_pair(raw: dict | str) -> dict:
    if isinstance(raw, str):
        if ":" not in raw:
            raise SessionError(f"pair {raw!r} must be backend:storage")
        backend, storage = raw.split(":", 1)
        raw = {"backend": backend, "storage": storage}
    backend = str(raw.get("backend") or "").strip().lower()
    storage = str(raw.get("storage") or "").strip().lower()
    if backend not in VALID_BACKENDS:
        raise SessionError(f"unknown backend {backend!r}")
    if storage not in VALID_STORAGES:
        raise SessionError(f"unknown storage {storage!r}")
    return {"backend": backend, "storage": storage}


def parse_pairs(raw: str | list | None) -> list[dict]:
    if raw is None:
        return [normalize_pair(item) for item in (load_defaults().get("pairs") or [])]
    if isinstance(raw, str):
        items = [part.strip() for part in raw.split(",") if part.strip()]
        return [normalize_pair(item) for item in items]
    return [normalize_pair(item) for item in raw]


def pair_services(pair: dict) -> list[str]:
    services = ["nats", pair["storage"], API_SERVICE[pair["backend"]], "prometheus"]
    if pair["storage"] == "timescaledb":
        services.append("postgres-exporter")
    return services


def new_session(
    why: str,
    profile: str | None = None,
    duration: str | None = None,
    transport: str | None = None,
    pairs: str | list | None = None,
    created_at: str | None = None,
) -> dict:
    why = (why or "").strip()
    if not why:
        raise SessionError("--why is required: every session needs a reason")
    defaults = load_defaults()
    seconds = parse_duration(duration or defaults["duration"])
    load = dict(defaults.get("load") or {})
    if profile:
        load["profile"] = profile
    if transport:
        load["transport"] = transport
    resolved = parse_pairs(pairs if pairs is not None else defaults.get("pairs"))
    if not resolved:
        raise SessionError("session must list at least one pair")
    created = created_at or utcnow()
    stamp = created.replace("-", "").replace(":", "")[:15]
    session_id = f"{stamp}-{load['profile']}"
    return {
        "id": session_id,
        "status": "planned",
        "when": {
            "created_at": created,
            "started_at": None,
            "finished_at": None,
        },
        "why": why,
        "what": {
            "duration": format_duration(seconds),
            "duration_seconds": seconds,
            "load": load,
            "pairs": resolved,
            "resources": deepcopy(defaults["resources"]),
        },
        "results": {"pairs": {}},
        "conclusions": "",
    }


def session_dir(session_id: str) -> Path:
    return SESSIONS_DIR / session_id


def session_path(session_id: str) -> Path:
    return session_dir(session_id) / "session.yaml"


def pair_dir(session_id: str, pair: dict) -> Path:
    return session_dir(session_id) / "pairs" / pair_slug(pair)


def save_session(session: dict) -> Path:
    path = session_path(session["id"])
    dump_yaml(path, session)
    rebuild_catalog()
    return path


def load_session(session_id: str) -> dict:
    path = session_path(session_id)
    if not path.exists():
        raise SessionError(f"session {session_id!r} not found")
    return load_yaml(path)


def list_session_ids() -> list[str]:
    return sorted(path.parent.name for path in SESSIONS_DIR.glob("*/session.yaml"))


def rebuild_catalog() -> dict:
    items = []
    for session_id in list_session_ids():
        session = load_session(session_id)
        what = session.get("what") or {}
        pairs = what.get("pairs") or []
        items.append(
            {
                "id": session["id"],
                "status": session.get("status"),
                "created_at": (session.get("when") or {}).get("created_at"),
                "why": session.get("why"),
                "profile": (what.get("load") or {}).get("profile"),
                "duration": what.get("duration"),
                "pairs": [pair_slug(normalize_pair(p)) for p in pairs],
                "path": str(session_path(session_id).relative_to(ROOT)).replace("\\", "/"),
            }
        )
    catalog = {"sessions": items}
    dump_yaml(CATALOG_PATH, catalog)
    return catalog


def compose_env(session: dict, pair: dict) -> dict[str, str]:
    what = session["what"]
    resources = what["resources"]
    load = what["load"]
    env = {
        "GO_API_STORAGE": pair["storage"],
        "PYTHON_API_STORAGE": pair["storage"],
        "PRISM_STORAGE": pair["storage"],
        "LOAD_PROFILE": str(load["profile"]),
        "GENERATOR_DURATION": str(what["duration_seconds"]),
        "GENERATOR_TARGET": str(load.get("transport") or ""),
        "GENERATOR_HTTP_URL": API_URL[pair["backend"]],
    }
    for group in RESOURCE_GROUPS:
        spec = resources[group]
        env[f"PRISM_RES_{group.upper()}_CPUS"] = str(spec["cpus"])
        env[f"PRISM_RES_{group.upper()}_MEMORY"] = str(spec["memory"])
    return env


def write_compose_env(session: dict, pair: dict) -> Path:
    path = pair_dir(session["id"], pair) / "compose.env"
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"{key}={value}" for key, value in compose_env(session, pair).items()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def parse_generator_output(text: str) -> dict:
    match = re.search(
        r"generator done profile=(?P<profile>\S+) written=(?P<written>\d+) "
        r"queries=(?P<queries>\d+) write_errors=(?P<write_errors>\d+) "
        r"query_errors=(?P<query_errors>\d+) elapsed=(?P<elapsed>[\d.]+)s "
        r"ingest_rate=(?P<ingest_rate>[\d.]+)/s",
        text,
    )
    if not match:
        return {"raw_excerpt": text[-1000:]}
    data = match.groupdict()
    return {
        "profile": data["profile"],
        "written": int(data["written"]),
        "queries": int(data["queries"]),
        "write_errors": int(data["write_errors"]),
        "query_errors": int(data["query_errors"]),
        "elapsed_seconds": float(data["elapsed"]),
        "ingest_rate": float(data["ingest_rate"]),
    }


def draft_conclusions(session: dict) -> str:
    what = session.get("what") or {}
    pairs = ((session.get("results") or {}).get("pairs") or {})
    lines = [
        "Черновик по цифрам. Заменить человеческим выводом.",
        "",
        f"Профиль {(what.get('load') or {}).get('profile')} за {what.get('duration')}, "
        f"пары по очереди с чистым стартом.",
    ]
    if not pairs:
        lines.append("Результатов пар нет.")
        return "\n".join(lines) + "\n"
    for slug, row in pairs.items():
        status = row.get("status")
        gen = row.get("generator") or {}
        prom = (row.get("prometheus") or [{}])[0] if row.get("prometheus") else {}
        lines.append(
            f"- {slug} [{status}]: "
            f"gen {gen.get('ingest_rate', 'n/a')} pts/s, "
            f"prom {prom.get('ingest_rate', 'n/a')} pts/s, "
            f"write p95 {prom.get('write_p95_seconds', 'n/a')}s, "
            f"errors {gen.get('write_errors', 'n/a')}."
        )
    return "\n".join(lines) + "\n"
