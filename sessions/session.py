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
VALID_BACKENDS = ("go", "python", "csharp", "rust")
VALID_STORAGES = ("timescaledb", "clickhouse", "questdb", "influxdb", "victoriametrics")
API_SERVICE = {"go": "go-api", "python": "python-api", "csharp": "csharp-api", "rust": "rust-api"}
API_URL = {
    "go": "http://go-api:8081",
    "python": "http://python-api:8082",
    "csharp": "http://csharp-api:8083",
    "rust": "http://rust-api:8084",
}
API_READY = {
    "go": "http://127.0.0.1:8081/readyz",
    "python": "http://127.0.0.1:8082/readyz",
    "csharp": "http://127.0.0.1:8083/readyz",
    "rust": "http://127.0.0.1:8084/readyz",
}
API_META = {
    "go": "http://127.0.0.1:8081/v1/meta",
    "python": "http://127.0.0.1:8082/v1/meta",
    "csharp": "http://127.0.0.1:8083/v1/meta",
    "rust": "http://127.0.0.1:8084/v1/meta",
}


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
    match = re.fullmatch(r"(\d+(?:\.\d+)?)(ms|s|m|h|d)", text)
    if not match:
        raise SessionError(f"invalid duration {raw!r}, expected 5m / 300s / 300")
    amount = float(match.group(1))
    mul = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400}[match.group(2)]
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


LAB_ARCHIVE_END = "2026-08-20T16:49:20Z"


def new_session(
    why: str,
    profile: str | None = None,
    duration: str | None = None,
    transport: str | None = None,
    pairs: str | list | None = None,
    created_at: str | None = None,
    reuse_volumes: bool = False,
    archive_end: str | None = None,
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
            "reuse_volumes": bool(reuse_volumes),
            "skip_seed": bool(reuse_volumes),
            "archive_end": (archive_end or LAB_ARCHIVE_END) if reuse_volumes else None,
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
        "CSHARP_API_STORAGE": pair["storage"],
        "RUST_API_STORAGE": pair["storage"],
        "PRISM_STORAGE": pair["storage"],
        "LOAD_PROFILE": str(load["profile"]),
        "GENERATOR_DURATION": str(what["duration_seconds"]),
        "GENERATOR_TARGET": str(load.get("transport") or ""),
        "GENERATOR_HTTP_URL": API_URL[pair["backend"]],
        "ARCHIVE_END": str(what.get("archive_end") or (session.get("when") or {}).get("created_at") or ""),
        "ARCHIVE_SEED": "0" if what.get("skip_seed") or what.get("reuse_volumes") else "1",
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
    out = {
        "profile": data["profile"],
        "written": int(data["written"]),
        "queries": int(data["queries"]),
        "write_errors": int(data["write_errors"]),
        "query_errors": int(data["query_errors"]),
        "elapsed_seconds": float(data["elapsed"]),
        "ingest_rate": float(data["ingest_rate"]),
    }
    seed = re.search(r"seed_elapsed=(?P<seed_elapsed>[\d.]+)s", text)
    if seed:
        out["seed_elapsed_seconds"] = float(seed.group("seed_elapsed"))
    query_elapsed = re.search(r"query_elapsed=(?P<query_elapsed>[\d.]+)s", text)
    if query_elapsed:
        out["query_elapsed_seconds"] = float(query_elapsed.group("query_elapsed"))
    return out


def _prom(record: dict) -> dict:
    rows = record.get("prometheus") or []
    return rows[0] if rows else {}


def _ms(seconds: float | None) -> float | None:
    if seconds is None:
        return None
    return round(seconds * 1000, 3)


def pair_scorecard(slug: str, record: dict) -> dict:
    gen = record.get("generator") or {}
    prom = _prom(record)
    stats = record.get("resources") or {}
    read_heavy = gen.get("profile") == "query-mix"
    ingest_rate = None if read_heavy else (
        gen.get("ingest_rate") if gen.get("ingest_rate") is not None else prom.get("ingest_rate")
    )
    return {
        "pair": slug,
        "status": record.get("status"),
        "ingest_rate": ingest_rate,
        "seed_written": gen.get("written") if read_heavy else None,
        "seed_elapsed_seconds": gen.get("seed_elapsed_seconds") if read_heavy else None,
        "write_errors": gen.get("write_errors"),
        "query_errors": gen.get("query_errors"),
        "queries": gen.get("queries"),
        "write_p95_ms": _ms(prom.get("write_p95_seconds")),
        "locf_p95_ms": _ms(prom.get("locf_p95_seconds")),
        "range_p95_ms": _ms(prom.get("range_p95_seconds")),
        "storage_write_p95_ms": _ms(prom.get("storage_write_p95_seconds")),
        "storage_locf_p95_ms": _ms(prom.get("storage_locf_p95_seconds")),
        "storage_range_p95_ms": _ms(prom.get("storage_range_p95_seconds")),
        "api_p95_ms": _ms(prom.get("api_p95_seconds")),
        "cpu_api": (stats.get("api") or {}).get("cpu"),
        "mem_api": (stats.get("api") or {}).get("mem"),
        "cpu_storage": (stats.get("storage") or {}).get("cpu"),
        "mem_storage": (stats.get("storage") or {}).get("mem"),
        "storage_bytes": (record.get("storage_size") or {}).get("bytes"),
        "storage_mib": (record.get("storage_size") or {}).get("mib"),
    }


def _rank(rows: list[dict], key: str, reverse: bool) -> list[str]:
    scored = [row for row in rows if row.get("status") == "completed" and row.get(key) is not None]
    scored.sort(key=lambda row: row[key], reverse=reverse)
    return [row["pair"] for row in scored]


def build_comparison(session: dict) -> dict:
    what = session.get("what") or {}
    profile = (what.get("load") or {}).get("profile")
    pairs = (session.get("results") or {}).get("pairs") or {}
    rows = [pair_scorecard(slug, record) for slug, record in pairs.items()]
    read_heavy = profile == "query-mix"
    if read_heavy:
        how = (
            "Одинаковый resource envelope и чистый старт на каждую пару. "
            "Сначала тот же архив (год, частые и редкие теги), затем только locf/range. "
            "Эффективнее та, что отвечает быстрее на locf и range без ошибок "
            "и занимает меньше места тем же архивом. "
            "storage_*_p95 показывает, где сидит задержка — в БД или в API. "
            "Seed в scorecard не входит."
        )
    else:
        how = (
            "Одинаковый resource envelope и чистый старт на каждую пару. "
            "Эффективнее та, что держит предложенный ingest без ошибок "
            "и отвечает быстрее на locf/range. "
            "storage_*_p95 показывает, где сидит задержка — в БД или в API."
        )
    ranks = {
        "locf_p95_ms": _rank(rows, "locf_p95_ms", reverse=False),
        "range_p95_ms": _rank(rows, "range_p95_ms", reverse=False),
        "storage_mib": _rank(rows, "storage_mib", reverse=False),
    }
    if not read_heavy:
        ranks = {
            "ingest_rate": _rank(rows, "ingest_rate", reverse=True),
            "write_p95_ms": _rank(rows, "write_p95_ms", reverse=False),
            **ranks,
        }
    return {
        "profile": profile,
        "duration": what.get("duration"),
        "envelope": what.get("resources"),
        "how": how,
        "primary": "read" if read_heavy else "write",
        "rows": rows,
        "ranks": ranks,
    }


def format_comparison(comparison: dict) -> str:
    lines = [
        f"Сравнение пар: профиль {comparison.get('profile')} за {comparison.get('duration')}.",
        comparison.get("how", ""),
        "",
        f"{'pair':<24} {'status':<10} {'ingest/s':>10} {'w_err':>6} {'q_err':>6} "
        f"{'w p95':>8} {'locf':>8} {'range':>8} {'disk':>8} {'cpu api':>8} {'cpu db':>8}",
    ]
    for row in comparison.get("rows") or []:
        lines.append(
            f"{row.get('pair', ''):<24} {str(row.get('status') or ''):<10} "
            f"{_fmt(row.get('ingest_rate'), 10)} {_fmt(row.get('write_errors'), 6)} "
            f"{_fmt(row.get('query_errors'), 6)} {_fmt(row.get('write_p95_ms'), 8)} "
            f"{_fmt(row.get('locf_p95_ms'), 8)} {_fmt(row.get('range_p95_ms'), 8)} "
            f"{_fmt(row.get('storage_mib'), 8)} "
            f"{_fmt(row.get('cpu_api'), 8)} {_fmt(row.get('cpu_storage'), 8)}"
        )
    ranks = comparison.get("ranks") or {}
    if ranks.get("ingest_rate"):
        lines.append("")
        lines.append("ingest: " + " > ".join(ranks["ingest_rate"]))
    if ranks.get("locf_p95_ms"):
        lines.append("locf:   " + " > ".join(ranks["locf_p95_ms"]) + "  (меньше p95 лучше)")
    if ranks.get("range_p95_ms"):
        lines.append("range:  " + " > ".join(ranks["range_p95_ms"]) + "  (меньше p95 лучше)")
    if ranks.get("storage_mib"):
        lines.append("disk:   " + " > ".join(ranks["storage_mib"]) + "  (меньше MiB лучше)")
    return "\n".join(lines) + "\n"


def _fmt(value, width: int) -> str:
    if value is None:
        return f"{'n/a':>{width}}"
    if isinstance(value, float):
        return f"{value:{width}.1f}"
    return f"{value!s:>{width}}"


def draft_conclusions(session: dict) -> str:
    comparison = build_comparison(session)
    return (
        "Черновик по цифрам. Заменить человеческим выводом.\n\n"
        + format_comparison(comparison)
    )
