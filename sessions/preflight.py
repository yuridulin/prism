"""Contract parity check: every backend must answer the same on every storage."""

from __future__ import annotations

import json
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from session import (
    API_HOST,
    API_META,
    API_READY,
    ROOT,
    SessionError,
    VALID_BACKENDS,
    VALID_STORAGES,
    pair_slug,
    volume_set_for_profile,
)

FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "contract-probe.yaml"
COMPOSE_FILES = ["-f", "docker-compose.yml", "-f", "docker-compose.session.yml"]
PROBE_TAG_IDS = (900001, 900002)


def load_fixture() -> dict:
    raw = yaml.safe_load(FIXTURE_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SessionError(f"{FIXTURE_PATH} must be a mapping")
    return raw


def _norm_date(raw: Any) -> str:
    text = str(raw).strip()
    if not text:
        return text
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_values(body: dict) -> dict:
    tags = body.get("tags") or []
    out_tags = []
    for tag in sorted(tags, key=lambda item: item.get("id", 0)):
        values = []
        for row in tag.get("values") or []:
            values.append(
                {
                    "date": _norm_date(row.get("date", "")),
                    "value": round(float(row.get("value", 0)), 4),
                    "quality": int(row.get("quality", 0)),
                }
            )
        values.sort(key=lambda item: item["date"])
        out_tags.append({"id": int(tag.get("id", 0)), "values": values})
    result: dict[str, Any] = {"tags": out_tags}
    key = body.get("requestKey")
    if key:
        result["requestKey"] = key
    return result


def http_json(method: str, url: str, payload: dict | list | None = None, timeout: float = 30.0) -> tuple[int, dict]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"} if payload is not None else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = {"error": {"code": "http_error", "message": text[:500]}}
        return exc.code, parsed


def _http_text(method: str, url: str, *, data: bytes | None = None, headers: dict | None = None, timeout: float = 30.0) -> tuple[int, str]:
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")


def _probe_tag_sql(storage: str) -> str:
    ids = ", ".join(str(i) for i in PROBE_TAG_IDS)
    if storage == "timescaledb":
        return f"DELETE FROM samples WHERE tag_id IN ({ids})"
    if storage == "clickhouse":
        return f"ALTER TABLE samples DELETE WHERE tag_id IN ({ids})"
    if storage == "questdb":
        symbols = ", ".join(f"'{i}'" for i in PROBE_TAG_IDS)
        return f"DELETE FROM samples WHERE tag_id IN ({symbols})"
    raise SessionError(f"unsupported probe cleanup storage {storage!r}")


def _probe_days(fixture: dict) -> set[str]:
    days: set[str] = set()
    for item in fixture.get("write") or []:
        raw = str(item.get("date", ""))
        if len(raw) >= 10:
            days.add(raw[:10])
    return days


def cleanup_probe_samples(storage: str, fixture: dict | None = None) -> None:
    """Remove prior probe writes so each backend compares on the same three points."""
    if storage in {"timescaledb", "clickhouse"}:
        sql = _probe_tag_sql(storage)
        if storage == "timescaledb":
            cmd = [
                "docker",
                "compose",
                *COMPOSE_FILES,
                "exec",
                "-T",
                "timescaledb",
                "psql",
                "-U",
                "prism",
                "-d",
                "prism",
                "-c",
                sql,
            ]
            proc = subprocess.run(cmd, cwd=ROOT, check=False, text=True, capture_output=True)
            if proc.returncode != 0:
                detail = (proc.stderr or proc.stdout or "").strip()
                raise SessionError(f"preflight cleanup {storage}: {detail}")
            return
        if storage == "clickhouse":
            sync_sql = f"{sql} SETTINGS mutations_sync=1"
            cmd = [
                "docker",
                "compose",
                *COMPOSE_FILES,
                "exec",
                "-T",
                "clickhouse",
                "clickhouse-client",
                "--user",
                "prism",
                "--password",
                "prism",
                "--database",
                "prism",
                "--query",
                sync_sql,
            ]
            proc = subprocess.run(cmd, cwd=ROOT, check=False, text=True, capture_output=True)
            if proc.returncode != 0:
                detail = (proc.stderr or proc.stdout or "").strip()
                raise SessionError(f"preflight cleanup {storage}: {detail}")
            return

    if storage == "questdb":
        for day in sorted(_probe_days(fixture or {})):
            query = urllib.parse.quote(f"ALTER TABLE samples DROP PARTITION LIST '{day}'")
            status, body = _http_text("GET", f"http://127.0.0.1:9001/exec?query={query}")
            if status >= 300:
                raise SessionError(f"preflight cleanup questdb HTTP {status}: {body[:300]}")
        time.sleep(0.5)
        return

    if storage == "influxdb":
        for tag_id in PROBE_TAG_IDS:
            payload = json.dumps(
                {
                    "start": "1970-01-01T00:00:00Z",
                    "stop": "2100-01-01T00:00:00Z",
                    "predicate": f'tag_id="{tag_id}"',
                }
            ).encode("utf-8")
            status, body = _http_text(
                "POST",
                "http://127.0.0.1:8086/api/v2/delete?org=prism&bucket=prism",
                data=payload,
                headers={
                    "Authorization": "Token prism-dev-token",
                    "Content-Type": "application/json",
                },
            )
            if status >= 300:
                raise SessionError(f"preflight cleanup influxdb HTTP {status}: {body[:300]}")
        return

    if storage == "victoriametrics":
        match = f'prism_sample{{tag_id=~"{"|".join(str(i) for i in PROBE_TAG_IDS)}"}}'
        status, body = _http_text(
            "POST",
            "http://127.0.0.1:8428/api/v1/admin/tsdb/delete_series?" + urllib.parse.urlencode({"match[]": match}),
        )
        if status >= 300:
            raise SessionError(f"preflight cleanup victoriametrics HTTP {status}: {body[:300]}")
        time.sleep(0.5)
        return

    raise SessionError(f"unsupported probe cleanup storage {storage!r}")


def check_meta(meta: dict, fixture: dict, backend: str, storage: str) -> list[str]:
    issues: list[str] = []
    if meta.get("backend") != backend:
        issues.append(f"meta.backend={meta.get('backend')!r} expected {backend!r}")
    if meta.get("storage") != storage:
        issues.append(f"meta.storage={meta.get('storage')!r} expected {storage!r}")
    if meta.get("contract") != fixture.get("contract"):
        issues.append(f"meta.contract={meta.get('contract')!r} expected {fixture.get('contract')!r}")
    ops = set(meta.get("ops") or [])
    required = set(fixture.get("required_ops") or [])
    missing = required - ops
    if missing:
        issues.append(f"meta.ops missing {sorted(missing)}")
    return issues


def probe_pair(
    backend: str,
    storage: str,
    fixture: dict,
    wait_ready,
    recreate_api,
    *,
    seed_write: bool,
) -> tuple[dict | None, list[str]]:
    """Point one API at storage, run probe, return normalized answers or issues."""
    issues: list[str] = []
    slug = pair_slug({"backend": backend, "storage": storage})
    recreate_api(backend, storage)
    wait_ready(API_READY[backend])

    status, meta = http_json("GET", API_META[backend])
    if status != 200:
        return None, [f"{slug}: /api/meta HTTP {status}"]
    issues.extend(check_meta(meta, fixture, backend, storage))
    if issues:
        return None, issues

    base = API_HOST[backend]
    if seed_write:
        try:
            cleanup_probe_samples(storage, fixture)
        except SessionError as exc:
            return None, [f"{slug}: {exc}"]
        status, write_resp = http_json("PUT", f"{base}/api/values", fixture["write"])
        if status != 200:
            return None, [f"{slug}: write HTTP {status} {write_resp}"]

    locf_payload = dict(fixture["locf"])
    status, locf_body = http_json("POST", f"{base}/api/values", locf_payload)
    if status != 200:
        return None, [f"{slug}: locf HTTP {status} {locf_body}"]

    range_payload = dict(fixture["range"])
    status, range_body = http_json("POST", f"{base}/api/values", range_payload)
    if status != 200:
        return None, [f"{slug}: range HTTP {status} {range_body}"]

    return {
        "locf": normalize_values(locf_body),
        "range": normalize_values(range_body),
    }, []


def run_preflight(
    session: dict,
    *,
    wait_ready,
    recreate_api,
    ensure_infra,
    backends: list[str] | None = None,
    storages: list[str] | None = None,
) -> dict:
    fixture = load_fixture()
    profile = (session.get("what") or {}).get("load") or {}
    volume_set = volume_set_for_profile(profile.get("profile", ""))
    backends = list(backends or VALID_BACKENDS)
    storages = list(storages or VALID_STORAGES)

    print(
        f"preflight: volume_set={volume_set} storages={','.join(storages)} "
        f"backends={','.join(backends)}",
        flush=True,
    )
    ensure_infra(session, volume_set, storages)

    by_storage: dict[str, dict] = {}
    issues: list[str] = []

    for storage in storages:
        baselines: dict[str, dict] = {}
        for backend in backends:
            slug = pair_slug({"backend": backend, "storage": storage})
            print(f"preflight: probe {slug}", flush=True)
            answers, probe_issues = probe_pair(
                backend,
                storage,
                fixture,
                wait_ready,
                recreate_api,
                seed_write=(backend == backends[0]),
            )
            issues.extend(probe_issues)
            if answers is None:
                continue
            if not baselines:
                baselines[backend] = answers
                continue
            ref_backend = next(iter(baselines))
            ref = baselines[ref_backend]
            if answers["locf"] != ref["locf"]:
                issues.append(f"{slug}: locf differs from {ref_backend}-{storage}")
            if answers["range"] != ref["range"]:
                issues.append(f"{slug}: range differs from {ref_backend}-{storage}")
        by_storage[storage] = deepcopy(baselines)

    report = {
        "ok": not issues,
        "volume_set": volume_set,
        "storages": storages,
        "backends": backends,
        "issues": issues,
        "baselines": by_storage,
    }
    if issues:
        print("preflight FAILED:", flush=True)
        for item in issues:
            print(f"  - {item}", flush=True)
        raise SessionError(f"preflight failed with {len(issues)} issue(s)")
    print("preflight OK", flush=True)
    return report
