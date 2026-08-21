"""Contract parity check: every backend must answer the same on every storage."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from session import (
    API_META,
    API_READY,
    SessionError,
    VALID_BACKENDS,
    VALID_STORAGES,
    pair_slug,
    volume_set_for_profile,
)

FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "contract-probe.yaml"


def load_fixture() -> dict:
    raw = yaml.safe_load(FIXTURE_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SessionError(f"{FIXTURE_PATH} must be a mapping")
    return raw


def normalize_values(body: dict) -> dict:
    tags = body.get("tags") or []
    out_tags = []
    for tag in sorted(tags, key=lambda item: item.get("id", 0)):
        values = []
        for row in tag.get("values") or []:
            values.append(
                {
                    "date": str(row.get("date", "")),
                    "value": round(float(row.get("value", 0)), 4),
                    "quality": int(row.get("quality", 0)),
                }
            )
        values.sort(key=lambda item: item["date"])
        out_tags.append({"id": int(tag.get("id", 0)), "values": values})
    result: dict[str, Any] = {"tags": out_tags}
    if body.get("requestKey") is not None:
        result["requestKey"] = body.get("requestKey")
    return result


def http_json(method: str, url: str, payload: dict | list | None = None, timeout: float = 30.0) -> tuple[int, dict]:
    import urllib.error
    import urllib.request

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

    base = API_URL[backend]
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
