"""Dispatch Prism sessions: one pair at a time, clean start each time."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from session import (  # noqa: E402
    API_META,
    API_READY,
    API_SERVICE,
    SessionError,
    build_comparison,
    draft_conclusions,
    dump_yaml,
    format_comparison,
    load_session,
    list_session_ids,
    new_session,
    normalize_pair,
    pair_dir,
    pair_services,
    pair_slug,
    parse_generator_output,
    rebuild_catalog,
    save_session,
    session_dir,
    utcnow,
    write_compose_env,
)

COMPOSE_FILES = ["-f", "docker-compose.yml", "-f", "docker-compose.session.yml"]


def compose(
    args: list[str],
    env_file: Path | None = None,
    capture: bool = True,
) -> subprocess.CompletedProcess:
    cmd = ["docker", "compose", *COMPOSE_FILES]
    if env_file is not None:
        cmd.extend(["--env-file", str(env_file)])
    cmd.extend(args)
    return subprocess.run(cmd, cwd=ROOT, check=False, text=True, capture_output=capture)


def compose_checked(args: list[str], env_file: Path | None = None, capture: bool = True) -> None:
    proc = compose(args, env_file, capture=capture)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise SessionError(f"docker compose {' '.join(args)} failed: {detail}")


def wipe_stack(env_file: Path | None = None) -> None:
    compose(["--profile", "load", "down", "-v", "--remove-orphans"], env_file, capture=False)


def http_get(url: str, timeout: float = 5.0) -> tuple[int, str]:
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")
    except Exception as exc:
        raise SessionError(f"request failed {url}: {exc}") from exc


def wait_ready(url: str, timeout: int = 180) -> None:
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        try:
            status, body = http_get(url)
            if status == 200:
                return
            last = f"{status} {body[:80]}"
        except SessionError as exc:
            last = str(exc)
        time.sleep(3)
    raise SessionError(f"not ready after {timeout}s ({url}: {last})")


def promql(query: str) -> list[dict]:
    qs = urllib.parse.urlencode({"query": query})
    _, body = http_get(f"http://127.0.0.1:9090/api/v1/query?{qs}", timeout=10)
    payload = json.loads(body)
    return (payload.get("data") or {}).get("result") or []


def scalar(row: dict) -> float | None:
    value = (row.get("value") or [None, None])[1]
    if value in {None, "NaN", "+Inf", "-Inf"}:
        return None
    return float(value)


def collect_prometheus(window: str, backend: str, storage: str) -> list[dict]:
    match = f'backend="{backend}",storage="{storage}"'

    def first(query: str) -> float | None:
        rows = promql(query)
        return scalar(rows[0]) if rows else None

    return [
        {
            "backend": backend,
            "storage": storage,
            "ingest_rate": first(f'sum(rate(prism_backend_items_total{{op="write",{match}}}[{window}]))'),
            "write_error_rate": first(
                f'sum(rate(prism_backend_ops_total{{op="write",result="error",{match}}}[{window}]))'
            ),
            "write_p95_seconds": first(
                "histogram_quantile(0.95, "
                f'sum by (le) (rate(prism_backend_op_duration_seconds_bucket{{op="write",{match}}}[{window}])))'
            ),
            "locf_p95_seconds": first(
                "histogram_quantile(0.95, "
                f'sum by (le) (rate(prism_backend_op_duration_seconds_bucket{{op="locf",{match}}}[{window}])))'
            ),
            "range_p95_seconds": first(
                "histogram_quantile(0.95, "
                f'sum by (le) (rate(prism_backend_op_duration_seconds_bucket{{op="range",{match}}}[{window}])))'
            ),
            "storage_write_p95_seconds": first(
                "histogram_quantile(0.95, "
                f'sum by (le) (rate(prism_storage_op_duration_seconds_bucket{{op="write",{match}}}[{window}])))'
            ),
            "storage_locf_p95_seconds": first(
                "histogram_quantile(0.95, "
                f'sum by (le) (rate(prism_storage_op_duration_seconds_bucket{{op="locf",{match}}}[{window}])))'
            ),
            "storage_range_p95_seconds": first(
                "histogram_quantile(0.95, "
                f'sum by (le) (rate(prism_storage_op_duration_seconds_bucket{{op="range",{match}}}[{window}])))'
            ),
            "api_p95_seconds": first(
                "histogram_quantile(0.95, "
                f'sum by (le) (rate(prism_api_request_duration_seconds_bucket{{{match}}}[{window}])))'
            ),
            "locf_error_rate": first(
                f'sum(rate(prism_backend_ops_total{{op="locf",result="error",{match}}}[{window}]))'
            ),
            "range_error_rate": first(
                f'sum(rate(prism_backend_ops_total{{op="range",result="error",{match}}}[{window}]))'
            ),
        }
    ]


def collect_docker_stats(pair: dict) -> dict:
    proc = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        return {"error": (proc.stderr or proc.stdout or "").strip()}
    api_name = API_SERVICE[pair["backend"]]
    storage_name = pair["storage"]
    out = {"raw": []}
    for line in (proc.stdout or "").splitlines():
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        name, cpu, mem = parts[0], parts[1], parts[2]
        row = {"name": name, "cpu": cpu, "mem": mem.split("/")[0].strip()}
        out["raw"].append(row)
        lowered = name.lower()
        if api_name in lowered:
            out["api"] = {"cpu": cpu, "mem": row["mem"]}
        if storage_name in lowered and "exporter" not in lowered:
            out["storage"] = {"cpu": cpu, "mem": row["mem"]}
    return out


def write_comparison(session: dict) -> Path:
    comparison = build_comparison(session)
    path = session_dir(session["id"]) / "comparison.yaml"
    dump_yaml(path, comparison)
    print(format_comparison(comparison), end="")
    return path


def collect_meta(backend: str) -> dict:
    try:
        status, body = http_get(API_META[backend])
        return json.loads(body) if status == 200 else {"status": status, "body": body}
    except SessionError as exc:
        return {"error": str(exc)}


def run_pair(session: dict, pair: dict) -> dict:
    slug = pair_slug(pair)
    work = pair_dir(session["id"], pair)
    env_file = write_compose_env(session, pair)
    services = pair_services(pair)
    record = {
        "backend": pair["backend"],
        "storage": pair["storage"],
        "status": "running",
        "when": {"started_at": utcnow(), "finished_at": None},
        "services": services,
    }
    print(f"pair {slug}: wipe -> up {', '.join(services)}")
    wipe_stack(env_file)
    try:
        compose_checked(["up", "-d", "--build", *services], env_file, capture=False)
        wait_ready(API_READY[pair["backend"]])
        wait_ready("http://127.0.0.1:9090/-/ready")
        window = session["what"]["duration"]
        print(f"pair {slug}: load {window}")
        proc = compose(
            ["--profile", "load", "run", "--rm", "--no-deps", "generator"],
            env_file,
            capture=True,
        )
        output = (proc.stdout or "") + "\n" + (proc.stderr or "")
        (work / "generator.out").write_text(output, encoding="utf-8")
        if proc.returncode != 0:
            raise SessionError(f"generator exited {proc.returncode}\n{output[-2000:]}")
        record["generator"] = parse_generator_output(output)
        record["meta"] = collect_meta(pair["backend"])
        try:
            record["prometheus"] = collect_prometheus(window, pair["backend"], pair["storage"])
        except SessionError as exc:
            record["prometheus_error"] = str(exc)
        record["resources"] = collect_docker_stats(pair)
        record["status"] = "completed"
    except Exception as exc:
        record["status"] = "failed"
        record["error"] = str(exc)
        print(f"pair {slug}: failed: {exc}", file=sys.stderr)
    finally:
        record["when"]["finished_at"] = utcnow()
        dump_yaml(work / "pair.yaml", record)
        print(f"pair {slug}: cleanup")
        wipe_stack(env_file)
    return record


def cmd_new(args: argparse.Namespace) -> int:
    session = new_session(
        why=args.why,
        profile=args.profile,
        duration=args.duration,
        transport=args.transport,
        pairs=args.pairs,
    )
    path = save_session(session)
    print(f"created {session['id']} ({path})")
    print("pairs: " + ", ".join(pair_slug(p) for p in session["what"]["pairs"]))
    if args.run:
        return cmd_run(argparse.Namespace(id=session["id"], from_pair=None, fail_fast=args.fail_fast))
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    session_id = args.id or _latest_planned()
    session = load_session(session_id)
    pairs = [normalize_pair(p) for p in session["what"]["pairs"]]
    if not pairs:
        raise SessionError("session has no pairs")
    slugs = [pair_slug(p) for p in pairs]
    start_at = 0
    if args.from_pair:
        if args.from_pair not in slugs:
            raise SessionError(f"unknown pair {args.from_pair!r}")
        start_at = slugs.index(args.from_pair)
    session["status"] = "running"
    session["when"]["started_at"] = session["when"].get("started_at") or utcnow()
    session.setdefault("results", {}).setdefault("pairs", {})
    save_session(session)
    print(
        f"dispatch {session_id} duration={session['what']['duration']} "
        f"profile={session['what']['load']['profile']} pairs={', '.join(slugs[start_at:])}"
    )
    failed = False
    try:
        for pair in pairs[start_at:]:
            slug = pair_slug(pair)
            record = run_pair(session, pair)
            session["results"]["pairs"][slug] = record
            session["results"]["comparison"] = build_comparison(session)
            session["conclusions"] = draft_conclusions(session)
            save_session(session)
            write_comparison(session)
            if record.get("status") != "completed":
                failed = True
                if args.fail_fast:
                    raise SessionError(f"pair {slug} failed, stopping")
        session["status"] = "failed" if failed else "completed"
        session["when"]["finished_at"] = utcnow()
        session["results"]["comparison"] = build_comparison(session)
        session["conclusions"] = draft_conclusions(session)
        save_session(session)
        write_comparison(session)
        print(f"{session['status']} {session_id}")
        return 1 if failed else 0
    except Exception as exc:
        session["status"] = "failed"
        session["when"]["finished_at"] = utcnow()
        session.setdefault("results", {})["error"] = str(exc)
        session["conclusions"] = draft_conclusions(session)
        save_session(session)
        wipe_stack()
        print(f"failed {session_id}: {exc}", file=sys.stderr)
        return 1


def _latest_planned() -> str:
    planned = [
        session_id
        for session_id in list_session_ids()
        if load_session(session_id).get("status") == "planned"
    ]
    if not planned:
        raise SessionError("no planned session; create one with `new --why ...`")
    return planned[-1]


def cmd_list(_: argparse.Namespace) -> int:
    rebuild_catalog()
    ids = list_session_ids()
    if not ids:
        print("no sessions yet")
        return 0
    for session_id in ids:
        session = load_session(session_id)
        what = session.get("what") or {}
        pairs = ",".join(pair_slug(normalize_pair(p)) for p in (what.get("pairs") or []))
        print(
            f"{session_id}  {session.get('status'):<10}  "
            f"{what.get('duration')}  {(what.get('load') or {}).get('profile')}  "
            f"[{pairs}]  {session.get('why')}"
        )
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    import yaml

    print(yaml.safe_dump(load_session(args.id), sort_keys=False, allow_unicode=True))
    return 0


def cmd_conclude(args: argparse.Namespace) -> int:
    session = load_session(args.id)
    session["conclusions"] = args.text if args.text.endswith("\n") else args.text + "\n"
    save_session(session)
    print(f"updated conclusions for {args.id}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prism pair dispatcher")
    sub = parser.add_subparsers(dest="command", required=True)

    new = sub.add_parser("new", help="create a session from starting defaults")
    new.add_argument("--why", required=True, help="why this run exists")
    new.add_argument("--profile", help="load profile (default: sessions/defaults.yaml)")
    new.add_argument("--duration", help="override starting duration, e.g. 5m")
    new.add_argument("--transport", help="nats or http")
    new.add_argument(
        "--pairs",
        help="comma-separated backend:storage, e.g. go:timescaledb,python:influxdb",
    )
    new.add_argument("--run", action="store_true", help="dispatch immediately")
    new.add_argument("--fail-fast", action="store_true")
    new.set_defaults(func=cmd_new)

    run = sub.add_parser("run", help="dispatch pairs: clean start, run, record, wipe")
    run.add_argument("id", nargs="?", help="session id; defaults to the latest planned")
    run.add_argument("--from-pair", help="resume from this pair slug, e.g. python-influxdb")
    run.add_argument("--fail-fast", action="store_true")
    run.set_defaults(func=cmd_run)

    listing = sub.add_parser("list", help="show the session catalog")
    listing.set_defaults(func=cmd_list)

    show = sub.add_parser("show", help="print one session record")
    show.add_argument("id")
    show.set_defaults(func=cmd_show)

    conclude = sub.add_parser("conclude", help="write the human conclusions")
    conclude.add_argument("id")
    conclude.add_argument("--text", required=True)
    conclude.set_defaults(func=cmd_conclude)

    compare = sub.add_parser("compare", help="print the pair scorecard")
    compare.add_argument("id")
    compare.set_defaults(func=cmd_compare)
    return parser


def cmd_compare(args: argparse.Namespace) -> int:
    session = load_session(args.id)
    comparison = (session.get("results") or {}).get("comparison") or build_comparison(session)
    print(format_comparison(comparison), end="")
    return 0


def main() -> int:
    os.chdir(ROOT)
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure:
            reconfigure(encoding="utf-8", errors="replace")
    args = build_parser().parse_args()
    try:
        return args.func(args)
    except SessionError as exc:
        print(exc, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
