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
STATUSES = ("planned", "running", "completed", "failed")


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


def deep_merge(base: dict, override: dict) -> dict:
    out = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = deep_merge(out[key], value)
        else:
            out[key] = deepcopy(value)
    return out


def new_session(
    why: str,
    profile: str | None = None,
    duration: str | None = None,
    transport: str | None = None,
    go_storage: str | None = None,
    python_storage: str | None = None,
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
    backends = dict(defaults.get("backends") or {})
    if go_storage:
        backends["go"] = go_storage
    if python_storage:
        backends["python"] = python_storage
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
            "backends": backends,
            "resources": deepcopy(defaults["resources"]),
        },
        "results": {},
        "conclusions": "",
    }


def session_dir(session_id: str) -> Path:
    return SESSIONS_DIR / session_id


def session_path(session_id: str) -> Path:
    return session_dir(session_id) / "session.yaml"


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
    ids = []
    for path in SESSIONS_DIR.glob("*/session.yaml"):
        ids.append(path.parent.name)
    return sorted(ids)


def rebuild_catalog() -> dict:
    items = []
    for session_id in list_session_ids():
        session = load_session(session_id)
        items.append(
            {
                "id": session["id"],
                "status": session.get("status"),
                "created_at": (session.get("when") or {}).get("created_at"),
                "why": session.get("why"),
                "profile": ((session.get("what") or {}).get("load") or {}).get("profile"),
                "duration": ((session.get("what") or {}).get("duration")),
                "path": str(session_path(session_id).relative_to(ROOT)).replace("\\", "/"),
            }
        )
    catalog = {"sessions": items}
    dump_yaml(CATALOG_PATH, catalog)
    return catalog


def compose_env(session: dict) -> dict[str, str]:
    what = session["what"]
    resources = what["resources"]
    load = what["load"]
    backends = what["backends"]
    env = {
        "GO_API_STORAGE": str(backends["go"]),
        "PYTHON_API_STORAGE": str(backends["python"]),
        "LOAD_PROFILE": str(load["profile"]),
        "GENERATOR_DURATION": str(what["duration_seconds"]),
        "GENERATOR_TARGET": str(load.get("transport") or ""),
    }
    for group in RESOURCE_GROUPS:
        spec = resources[group]
        env[f"PRISM_RES_{group.upper()}_CPUS"] = str(spec["cpus"])
        env[f"PRISM_RES_{group.upper()}_MEMORY"] = str(spec["memory"])
    return env


def write_compose_env(session: dict) -> Path:
    path = session_dir(session["id"]) / "compose.env"
    lines = [f"{key}={value}" for key, value in compose_env(session).items()]
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
    results = session.get("results") or {}
    gen = results.get("generator") or {}
    prom = results.get("prometheus") or []
    lines = [
        "Черновик по цифрам. Заменить человеческим выводом.",
        "",
        f"Профиль {((session.get('what') or {}).get('load') or {}).get('profile')} "
        f"за {((session.get('what') or {}).get('duration'))}.",
    ]
    if "ingest_rate" in gen:
        lines.append(
            f"Генератор: {gen.get('written', 0)} точек, {gen.get('ingest_rate')} pts/s, "
            f"ошибок записи {gen.get('write_errors', 0)}, запросов {gen.get('queries', 0)}."
        )
    for row in prom:
        lines.append(
            f"{row.get('backend')}/{row.get('storage')}: "
            f"ingest {row.get('ingest_rate', 'n/a')} pts/s, "
            f"write p95 {row.get('write_p95_seconds', 'n/a')}s, "
            f"errors {row.get('write_error_rate', 'n/a')}/s."
        )
    if not prom and "ingest_rate" not in gen:
        lines.append("Метрик нет — прогон не собрал generator/Prometheus.")
    return "\n".join(lines) + "\n"
