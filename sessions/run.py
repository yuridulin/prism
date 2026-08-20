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
    LAB_ARCHIVE_END,
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

STORAGE_DATA_PATH = {
    "timescaledb": "/var/lib/postgresql/data",
    "clickhouse": "/var/lib/clickhouse",
    "questdb": "/var/lib/questdb",
    "influxdb": "/var/lib/influxdb2",
    "victoriametrics": "/victoria-metrics-data",
}


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


def collect_storage_size(pair: dict, env_file: Path | None) -> dict:
    storage = pair["storage"]
    dest = STORAGE_DATA_PATH.get(storage)
    if not dest:
        return {"error": f"no data path for {storage}"}
    proc = compose(["ps", "-q", storage], env_file)
    cid = (proc.stdout or "").strip().splitlines()
    cid = cid[0] if cid else ""
    if not cid:
        return {"error": f"no container for {storage}"}
    inspect = subprocess.run(
        ["docker", "inspect", cid, "--format", "{{json .Mounts}}"],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    if inspect.returncode != 0:
        return {"error": (inspect.stderr or inspect.stdout or "").strip()}
    try:
        mounts = json.loads(inspect.stdout or "[]")
    except json.JSONDecodeError as exc:
        return {"error": f"inspect mounts: {exc}"}
    volume = None
    source = None
    for mount in mounts:
        if mount.get("Destination") == dest:
            volume = mount.get("Name") or ""
            source = mount.get("Source") or ""
            break
    if not volume and not source:
        return {"error": f"no mount at {dest}", "mounts": mounts}
    bind = f"{volume}:/measure:ro" if volume else f"{source}:/measure:ro"
    du = subprocess.run(
        ["docker", "run", "--rm", "-v", bind, "alpine:3.20", "du", "-sb", "/measure"],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    if du.returncode != 0:
        return {
            "error": (du.stderr or du.stdout or "").strip(),
            "volume": volume or None,
            "path": dest,
        }
    raw = (du.stdout or "").strip().split()
    try:
        nbytes = int(raw[0])
    except (IndexError, ValueError):
        return {"error": f"du output {du.stdout!r}", "volume": volume or None, "path": dest}
    return {
        "bytes": nbytes,
        "mib": round(nbytes / (1024 * 1024), 1),
        "volume": volume or None,
        "path": dest,
    }


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
    keep = bool((session.get("what") or {}).get("reuse_volumes"))
    record = {
        "backend": pair["backend"],
        "storage": pair["storage"],
        "status": "running",
        "when": {"started_at": utcnow(), "finished_at": None},
        "services": services,
        "reuse_volumes": keep,
    }
    if keep:
        print(f"pair {slug}: reuse -> up {', '.join(services)}", flush=True)
    else:
        print(f"pair {slug}: wipe -> up {', '.join(services)}", flush=True)
        wipe_stack(env_file)
    try:
        compose_checked(["up", "-d", "--build", *services], env_file, capture=False)
        if keep:
            compose_checked(
                ["up", "-d", "--build", "--force-recreate", "--no-deps", API_SERVICE[pair["backend"]]],
                env_file,
                capture=False,
            )
        wait_ready(API_READY[pair["backend"]])
        wait_ready("http://127.0.0.1:9090/-/ready")
        if keep:
            # Host :808x can be up before Docker DNS publishes the API alias.
            time.sleep(5)
        window = session["what"]["duration"]
        print(f"pair {slug}: load {window}", flush=True)
        proc = compose(
            ["--profile", "load", "run", "--rm", "--build", "--no-deps", "generator"],
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
        record["storage_size"] = collect_storage_size(pair, env_file)
        size = record["storage_size"]
        if size.get("bytes") is not None:
            print(f"pair {slug}: disk {size['mib']} MiB ({size['bytes']} bytes)", flush=True)
        elif size.get("error"):
            print(f"pair {slug}: disk measure failed: {size['error']}", file=sys.stderr)
        record["status"] = "completed"
    except Exception as exc:
        record["status"] = "failed"
        record["error"] = str(exc)
        print(f"pair {slug}: failed: {exc}", file=sys.stderr)
    finally:
        record["when"]["finished_at"] = utcnow()
        dump_yaml(work / "pair.yaml", record)
        if keep:
            print(f"pair {slug}: done (volumes kept)", flush=True)
        else:
            print(f"pair {slug}: cleanup", flush=True)
            wipe_stack(env_file)
    return record


def cmd_new(args: argparse.Namespace) -> int:
    session = new_session(
        why=args.why,
        profile=args.profile,
        duration=args.duration,
        transport=args.transport,
        pairs=args.pairs,
        reuse_volumes=args.keep,
        archive_end=args.archive_end,
    )
    path = save_session(session)
    print(f"created {session['id']} ({path})")
    print("pairs: " + ", ".join(pair_slug(p) for p in session["what"]["pairs"]))
    if args.run:
        return cmd_run(
            argparse.Namespace(
                id=session["id"],
                from_pair=None,
                only_pair=None,
                fail_fast=args.fail_fast,
                keep=args.keep,
            )
        )
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    session_id = args.id or _latest_planned()
    session = load_session(session_id)
    pairs = [normalize_pair(p) for p in session["what"]["pairs"]]
    if not pairs:
        raise SessionError("session has no pairs")
    slugs = [pair_slug(p) for p in pairs]
    start_at = 0
    stop_at = len(pairs)
    if args.only_pair:
        if args.only_pair not in slugs:
            raise SessionError(f"unknown pair {args.only_pair!r}")
        start_at = slugs.index(args.only_pair)
        stop_at = start_at + 1
    elif args.from_pair:
        if args.from_pair not in slugs:
            raise SessionError(f"unknown pair {args.from_pair!r}")
        start_at = slugs.index(args.from_pair)
    if getattr(args, "keep", False):
        session["what"]["reuse_volumes"] = True
        session["what"]["skip_seed"] = True
        session["what"].setdefault("archive_end", LAB_ARCHIVE_END)
    session["status"] = "running"
    session["when"]["started_at"] = session["when"].get("started_at") or utcnow()
    session.setdefault("results", {}).setdefault("pairs", {})
    save_session(session)
    print(
        f"dispatch {session_id} duration={session['what']['duration']} "
        f"profile={session['what']['load']['profile']} pairs={', '.join(slugs[start_at:stop_at])}",
        flush=True,
    )
    failed = False
    try:
        for pair in pairs[start_at:stop_at]:
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
        stored = session.get("results", {}).get("pairs") or {}
        any_failed = failed or any(
            (stored.get(pair_slug(normalize_pair(p))) or {}).get("status") != "completed"
            for p in session["what"]["pairs"]
        )
        session["status"] = "failed" if any_failed else "completed"
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
        if not (session.get("what") or {}).get("reuse_volumes"):
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
    new.add_argument("--keep", action="store_true", help="reuse volumes, skip wipe and archive seed")
    new.add_argument("--archive-end", help="UTC end of the existing archive (required for --keep query-mix)")
    new.set_defaults(func=cmd_new)

    run = sub.add_parser("run", help="dispatch pairs: clean start, run, record, wipe")
    run.add_argument("id", nargs="?", help="session id; defaults to the latest planned")
    run.add_argument("--from-pair", help="resume from this pair slug, e.g. python-influxdb")
    run.add_argument("--only-pair", help="run a single pair slug and keep the rest of the session")
    run.add_argument("--fail-fast", action="store_true")
    run.add_argument("--keep", action="store_true", help="reuse volumes, skip wipe and archive seed")
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
