"""Create and run Prism sessions through Docker."""

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
    SessionError,
    draft_conclusions,
    load_session,
    list_session_ids,
    new_session,
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


def http_get(url: str, timeout: float = 5.0) -> tuple[int, str]:
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as exc:
        raise SessionError(f"request failed {url}: {exc}") from exc


def wait_ready(timeout: int = 180) -> None:
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        try:
            go_status, _ = http_get("http://127.0.0.1:8081/readyz")
            py_status, _ = http_get("http://127.0.0.1:8082/readyz")
            if go_status == 200 and py_status == 200:
                return
            last = f"go={go_status} python={py_status}"
        except SessionError as exc:
            last = str(exc)
        time.sleep(3)
    raise SessionError(f"APIs not ready after {timeout}s ({last})")


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


def collect_prometheus(window: str) -> list[dict]:
    def by_pair(query: str) -> dict[tuple[str, str], float | None]:
        out: dict[tuple[str, str], float | None] = {}
        for row in promql(query):
            metric = row.get("metric") or {}
            key = (str(metric.get("backend") or ""), str(metric.get("storage") or ""))
            out[key] = scalar(row)
        return out

    ingest = by_pair(f'sum by (backend, storage) (rate(prism_backend_items_total{{op="write"}}[{window}]))')
    errors = by_pair(
        f'sum by (backend, storage) (rate(prism_backend_ops_total{{op="write",result="error"}}[{window}]))'
    )
    p95 = by_pair(
        "histogram_quantile(0.95, "
        f'sum by (le, backend, storage) (rate(prism_backend_op_duration_seconds_bucket{{op="write"}}[{window}])))'
    )
    keys = sorted(set(ingest) | set(errors) | set(p95))
    return [
        {
            "backend": backend,
            "storage": storage,
            "ingest_rate": ingest.get((backend, storage)),
            "write_error_rate": errors.get((backend, storage)),
            "write_p95_seconds": p95.get((backend, storage)),
        }
        for backend, storage in keys
    ]


def collect_meta() -> dict:
    out = {}
    for name, url in (("go", "http://127.0.0.1:8081/v1/meta"), ("python", "http://127.0.0.1:8082/v1/meta")):
        try:
            status, body = http_get(url)
            out[name] = json.loads(body) if status == 200 else {"status": status, "body": body}
        except SessionError as exc:
            out[name] = {"error": str(exc)}
    return out


def cmd_new(args: argparse.Namespace) -> int:
    session = new_session(
        why=args.why,
        profile=args.profile,
        duration=args.duration,
        transport=args.transport,
        go_storage=args.go_storage,
        python_storage=args.python_storage,
    )
    path = save_session(session)
    write_compose_env(session)
    print(f"created {session['id']} ({path})")
    if args.run:
        return cmd_run(argparse.Namespace(id=session["id"], down=args.down, no_up=False))
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    session_id = args.id or _latest_planned()
    session = load_session(session_id)
    env_file = write_compose_env(session)
    work = session_dir(session_id)
    session["status"] = "running"
    session["when"]["started_at"] = utcnow()
    save_session(session)
    print(f"running {session_id} duration={session['what']['duration']} profile={session['what']['load']['profile']}")
    try:
        if not args.no_up:
            print("starting stack with session resource envelope")
            compose_checked(["up", "-d", "--build"], env_file, capture=False)
            wait_ready()
        window = session["what"]["duration"]
        proc = compose(
            ["--profile", "load", "run", "--rm", "--no-deps", "generator"],
            env_file,
            capture=True,
        )
        output = (proc.stdout or "") + "\n" + (proc.stderr or "")
        (work / "generator.out").write_text(output, encoding="utf-8")
        if proc.returncode != 0:
            raise SessionError(f"generator exited {proc.returncode}\n{output[-2000:]}")
        results = {
            "generator": parse_generator_output(output),
            "meta": collect_meta(),
        }
        try:
            results["prometheus"] = collect_prometheus(window)
        except SessionError as exc:
            results["prometheus_error"] = str(exc)
        session["results"] = results
        if not session.get("conclusions"):
            session["conclusions"] = draft_conclusions(session)
        session["status"] = "completed"
        session["when"]["finished_at"] = utcnow()
        save_session(session)
        print(f"completed {session_id}")
        if args.down:
            compose(["--profile", "load", "down"], env_file)
        return 0
    except Exception as exc:
        session["status"] = "failed"
        session["when"]["finished_at"] = utcnow()
        session.setdefault("results", {})["error"] = str(exc)
        if not session.get("conclusions"):
            session["conclusions"] = f"Прогон не завершился: {exc}\n"
        save_session(session)
        print(f"failed {session_id}: {exc}", file=sys.stderr)
        return 1


def _latest_planned() -> str:
    planned = []
    for session_id in list_session_ids():
        session = load_session(session_id)
        if session.get("status") == "planned":
            planned.append(session_id)
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
        print(
            f"{session_id}  {session.get('status'):<10}  "
            f"{what.get('duration')}  {((what.get('load') or {}).get('profile'))}  "
            f"{session.get('why')}"
        )
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    session = load_session(args.id)
    print(yaml_dump(session))
    return 0


def yaml_dump(data: dict) -> str:
    import yaml

    return yaml.safe_dump(data, sort_keys=False, allow_unicode=True)


def cmd_conclude(args: argparse.Namespace) -> int:
    session = load_session(args.id)
    session["conclusions"] = args.text if args.text.endswith("\n") else args.text + "\n"
    save_session(session)
    print(f"updated conclusions for {args.id}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prism experiment sessions")
    sub = parser.add_subparsers(dest="command", required=True)

    new = sub.add_parser("new", help="create a session from starting defaults")
    new.add_argument("--why", required=True, help="why this run exists")
    new.add_argument("--profile", help="load profile (default: sessions/defaults.yaml)")
    new.add_argument("--duration", help="override starting duration, e.g. 5m")
    new.add_argument("--transport", help="nats or http")
    new.add_argument("--go-storage", help="timescaledb|clickhouse|influxdb|victoriametrics")
    new.add_argument("--python-storage", help="timescaledb|clickhouse|influxdb|victoriametrics")
    new.add_argument("--run", action="store_true", help="start the Docker run immediately")
    new.add_argument("--down", action="store_true", help="compose down after a successful run")
    new.set_defaults(func=cmd_new)

    run = sub.add_parser("run", help="execute a planned session via Docker")
    run.add_argument("id", nargs="?", help="session id; defaults to the latest planned")
    run.add_argument("--no-up", action="store_true", help="do not recreate the stack")
    run.add_argument("--down", action="store_true")
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
    return parser


def main() -> int:
    os.chdir(ROOT)
    args = build_parser().parse_args()
    try:
        return args.func(args)
    except SessionError as exc:
        print(exc, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
