"""Dispatch Prism sessions: parallel API replicas per backend by default."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    API_CONTAINER_PORT,
    API_META,
    API_READY,
    API_SERVICE,
    API_URL,
    LAB_ARCHIVE_END,
    SessionError,
    backend_stack_services,
    build_comparison,
    draft_conclusions,
    dump_yaml,
    format_comparison,
    group_pairs_by_backend,
    load_session,
    list_session_ids,
    new_session,
    normalize_pair,
    pair_dir,
    pair_services,
    pair_slug,
    parallel_container_name,
    parallel_host_port,
    parse_generator_output,
    rebuild_catalog,
    save_session,
    session_dir,
    storage_stack_services,
    storage_volume_name,
    utcnow,
    volume_set_for,
    write_compose_env,
)
from preflight import run_preflight  # noqa: E402

COMPOSE_FILES = ["-f", "docker-compose.yml", "-f", "docker-compose.session.yml"]
PROMETHEUS_BASE = ROOT / "infra" / "prometheus" / "prometheus.yml"

STORAGE_DATA_PATH = {
    "timescaledb": "/var/lib/postgresql/data",
    "questdb": "/var/lib/questdb",
    "victoriametrics": "/victoria-metrics-data",
}


def compose(
    args: list[str],
    env_file: Path | None = None,
    capture: bool = True,
    extra_files: list[Path] | None = None,
) -> subprocess.CompletedProcess:
    cmd = ["docker", "compose", *COMPOSE_FILES]
    for path in extra_files or []:
        cmd.extend(["-f", str(path)])
    if env_file is not None:
        cmd.extend(["--env-file", str(env_file)])
    cmd.extend(args)
    if os.environ.get("PRISM_NO_BUILD") == "1":
        cmd = [part for part in cmd if part != "--build"]
    return subprocess.run(cmd, cwd=ROOT, check=False, text=True, capture_output=capture)


def compose_checked(
    args: list[str],
    env_file: Path | None = None,
    capture: bool = True,
    extra_files: list[Path] | None = None,
) -> None:
    proc = compose(args, env_file, capture=capture, extra_files=extra_files)
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


def http_post(url: str, payload: dict, timeout: float = 30.0) -> tuple[int, str]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")
    except Exception as exc:
        raise SessionError(f"request failed {url}: {exc}") from exc


def archive_seeded(api_base: str, archive_end: str) -> bool:
    for tag_id in (1, 9):
        status, body = http_post(
            f"{api_base.rstrip('/')}/api/values",
            {"tagsId": [tag_id], "exact": archive_end},
        )
        if status != 200:
            return False
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            return False
        tags = payload.get("tags") or []
        if not tags or not (tags[0].get("values") or []):
            return False
    return True


def wipe_storage_volume(storage: str, volume_set: str, env_file: Path) -> None:
    compose(["rm", "-sf", storage], env_file, capture=True)
    name = storage_volume_name(storage, volume_set)
    subprocess.run(["docker", "volume", "rm", "-f", name], cwd=ROOT, check=False, capture_output=True)


def should_reset_storage_volume(session: dict) -> bool:
    what = session.get("what") or {}
    if what.get("reuse_volumes"):
        return False
    return volume_set_for(session) != "data"


def resolve_skip_seed(session: dict, pair: dict, api_base: str | None = None) -> bool:
    what = session.get("what") or {}
    if what.get("skip_seed") or what.get("reuse_volumes"):
        return True
    profile = (what.get("load") or {}).get("profile")
    if profile != "query-mix" and volume_set_for(session) != "data":
        return False
    archive_end = str(what.get("archive_end") or LAB_ARCHIVE_END)
    base = api_base or API_URL[pair["backend"]]
    if archive_seeded(base, archive_end):
        return True
    return False


def update_compose_seed_flag(env_file: Path, skip_seed: bool) -> None:
    lines = env_file.read_text(encoding="utf-8").splitlines()
    out = []
    replaced = False
    for line in lines:
        if line.startswith("ARCHIVE_SEED="):
            out.append(f"ARCHIVE_SEED={'0' if skip_seed else '1'}")
            replaced = True
        else:
            out.append(line)
    if not replaced:
        out.append(f"ARCHIVE_SEED={'0' if skip_seed else '1'}")
    env_file.write_text("\n".join(out) + "\n", encoding="utf-8")


def recreate_api(backend: str, env_file: Path) -> None:
    compose_checked(
        ["up", "-d", "--build", "--force-recreate", "--no-deps", API_SERVICE[backend]],
        env_file,
        capture=False,
    )


def ensure_backend_stack(session: dict, backend: str, env_file: Path) -> None:
    storages = [p["storage"] for p in ((session.get("what") or {}).get("pairs") or []) if p.get("backend") == backend]
    if not storages:
        storages = None
    services = backend_stack_services(backend, storages)
    compose_checked(["up", "-d", "--build", *services], env_file, capture=False)
    wait_ready("http://127.0.0.1:9090/-/ready")


def ensure_infra_for_preflight(session: dict, volume_set: str, storages: list[str]) -> None:
    dummy_backend = next(
        (p["backend"] for p in ((session.get("what") or {}).get("pairs") or [])),
        "csharp",
    )
    pair = normalize_pair({"backend": dummy_backend, "storage": storages[0]})
    env_file = write_compose_env(session, pair)
    lines = env_file.read_text(encoding="utf-8").splitlines()
    if not any(line.startswith("PRISM_VOLUME_SET=") for line in lines):
        lines.append(f"PRISM_VOLUME_SET={volume_set}")
    else:
        lines = [
            (f"PRISM_VOLUME_SET={volume_set}" if line.startswith("PRISM_VOLUME_SET=") else line)
            for line in lines
        ]
    env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    services = ["nats", "prometheus", *storages]
    if "timescaledb" in storages:
        services.append("postgres-exporter")
    compose_checked(["up", "-d", "--build", *services], env_file, capture=False)
    wait_ready("http://127.0.0.1:9090/-/ready")


def preflight_recreate_api(backend: str, storage: str, session: dict) -> None:
    pair = normalize_pair({"backend": backend, "storage": storage})
    env_file = write_compose_env(session, pair)
    recreate_api(backend, env_file)


def run_session_preflight(session: dict) -> None:
    def recreate(backend: str, storage: str) -> None:
        preflight_recreate_api(backend, storage, session)

    run_preflight(
        session,
        wait_ready=wait_ready,
        recreate_api=recreate,
        ensure_infra=ensure_infra_for_preflight,
    )


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


def measure_pair(session: dict, pair: dict, *, stack_ready: bool = False) -> dict:
    slug = pair_slug(pair)
    work = pair_dir(session["id"], pair)
    env_file = write_compose_env(session, pair)
    keep = bool((session.get("what") or {}).get("reuse_volumes"))
    volume_set = volume_set_for(session)
    skip_seed = resolve_skip_seed(session, pair)
    update_compose_seed_flag(env_file, skip_seed)
    record = {
        "backend": pair["backend"],
        "storage": pair["storage"],
        "status": "running",
        "when": {"started_at": utcnow(), "finished_at": None},
        "services": pair_services(pair),
        "reuse_volumes": keep,
        "volume_set": volume_set,
        "skip_seed": skip_seed,
        "dispatch": (session.get("what") or {}).get("dispatch") or "by-backend",
    }
    try:
        if should_reset_storage_volume(session):
            print(f"pair {slug}: reset volume {storage_volume_name(pair['storage'], volume_set)}", flush=True)
            compose(["stop", pair["storage"]], env_file, capture=True)
            wipe_storage_volume(pair["storage"], volume_set, env_file)
            compose_checked(["up", "-d", pair["storage"]], env_file, capture=False)
        if not stack_ready:
            if keep:
                print(f"pair {slug}: reuse -> up {', '.join(record['services'])}", flush=True)
            else:
                print(f"pair {slug}: wipe -> up {', '.join(record['services'])}", flush=True)
                wipe_stack(env_file)
            compose_checked(["up", "-d", "--build", *record["services"]], env_file, capture=False)
            wait_ready(API_READY[pair["backend"]])
            wait_ready("http://127.0.0.1:9090/-/ready")
            if keep:
                time.sleep(5)
        else:
            print(f"pair {slug}: measure on {pair['storage']}", flush=True)
        if skip_seed:
            print(f"pair {slug}: archive seed skipped", flush=True)
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
    return record


def run_pair_isolated(session: dict, pair: dict) -> dict:
    slug = pair_slug(pair)
    keep = bool((session.get("what") or {}).get("reuse_volumes"))
    record = measure_pair(session, pair, stack_ready=False)
    if keep:
        print(f"pair {slug}: done (volumes kept)", flush=True)
    else:
        env_file = write_compose_env(session, pair)
        print(f"pair {slug}: cleanup", flush=True)
        wipe_stack(env_file)
    return record


def remove_container(name: str) -> None:
    subprocess.run(["docker", "rm", "-f", name], cwd=ROOT, check=False, capture_output=True)


def write_parallel_prometheus_config(
    path: Path,
    backend: str,
    replicas: list[tuple[dict, str]],
) -> None:
    base = PROMETHEUS_BASE.read_text(encoding="utf-8")
    blocks = [base.rstrip(), ""]
    for pair, container in replicas:
        port = API_CONTAINER_PORT[pair["backend"]]
        blocks.append(
            "  - job_name: {job}\n"
            "    static_configs:\n"
            '      - targets: ["{target}:{port}"]\n'
            "        labels:\n"
            '          backend: "{backend}"\n'
            '          storage: "{storage}"\n'
            "          layer: api".format(
                job=f"parallel-{pair_slug(pair)}",
                target=container,
                port=port,
                backend=pair["backend"],
                storage=pair["storage"],
            )
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(blocks) + "\n", encoding="utf-8")


def write_prometheus_compose_override(path: Path, prom_config: Path) -> None:
    rel = prom_config.relative_to(ROOT).as_posix()
    path.write_text(
        "services:\n"
        "  prometheus:\n"
        "    volumes:\n"
        f"      - ./{rel}:/etc/prometheus/prometheus.yml:ro\n",
        encoding="utf-8",
    )


def start_parallel_api(
    pair: dict,
    env_file: Path,
    container: str,
    host_port: int,
) -> None:
    remove_container(container)
    service = API_SERVICE[pair["backend"]]
    internal = API_CONTAINER_PORT[pair["backend"]]
    compose_checked(
        [
            "run",
            "-d",
            "--name",
            container,
            "-p",
            f"127.0.0.1:{host_port}:{internal}",
            "--no-deps",
            service,
        ],
        env_file,
        capture=True,
    )


def collect_meta_url(meta_url: str) -> dict:
    try:
        status, body = http_get(meta_url)
        return json.loads(body) if status == 200 else {"status": status, "body": body}
    except SessionError as exc:
        return {"error": str(exc)}


def collect_docker_stats_named(api_container: str, storage: str) -> dict:
    proc = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        return {"error": (proc.stderr or proc.stdout or "").strip()}
    out = {"raw": []}
    for line in (proc.stdout or "").splitlines():
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        name, cpu, mem = parts[0], parts[1], parts[2]
        row = {"name": name, "cpu": cpu, "mem": mem.split("/")[0].strip()}
        out["raw"].append(row)
        if name == api_container:
            out["api"] = {"cpu": cpu, "mem": row["mem"]}
        if storage in name.lower() and "exporter" not in name.lower():
            out["storage"] = {"cpu": cpu, "mem": row["mem"]}
    return out


def run_generator(session: dict, pair: dict, env_file: Path) -> tuple[str, int]:
    window = session["what"]["duration"]
    slug = pair_slug(pair)
    print(f"pair {slug}: load {window}", flush=True)
    proc = compose(
        ["--profile", "load", "run", "--rm", "--build", "--no-deps", "generator"],
        env_file,
        capture=True,
    )
    output = (proc.stdout or "") + "\n" + (proc.stderr or "")
    return output, proc.returncode


def finalize_pair_record(
    session: dict,
    pair: dict,
    *,
    env_file: Path,
    skip_seed: bool,
    generator_output: str,
    generator_rc: int,
    meta_url: str,
    api_container: str,
    dispatch: str,
) -> dict:
    slug = pair_slug(pair)
    work = pair_dir(session["id"], pair)
    keep = bool((session.get("what") or {}).get("reuse_volumes"))
    record = {
        "backend": pair["backend"],
        "storage": pair["storage"],
        "status": "running",
        "when": {"started_at": utcnow(), "finished_at": None},
        "services": pair_services(pair),
        "reuse_volumes": keep,
        "volume_set": volume_set_for(session),
        "skip_seed": skip_seed,
        "dispatch": dispatch,
        "api_container": api_container,
    }
    (work / "generator.out").write_text(generator_output, encoding="utf-8")
    if generator_rc != 0:
        record["status"] = "failed"
        record["error"] = f"generator exited {generator_rc}\n{generator_output[-2000:]}"
    else:
        record["generator"] = parse_generator_output(generator_output)
        record["meta"] = collect_meta_url(meta_url)
        window = session["what"]["duration"]
        try:
            record["prometheus"] = collect_prometheus(window, pair["backend"], pair["storage"])
        except SessionError as exc:
            record["prometheus_error"] = str(exc)
        record["resources"] = collect_docker_stats_named(api_container, pair["storage"])
        record["storage_size"] = collect_storage_size(pair, env_file)
        size = record["storage_size"]
        if size.get("bytes") is not None:
            print(f"pair {slug}: disk {size['mib']} MiB ({size['bytes']} bytes)", flush=True)
        elif size.get("error"):
            print(f"pair {slug}: disk measure failed: {size['error']}", file=sys.stderr)
        record["status"] = "completed"
    record["when"]["finished_at"] = utcnow()
    dump_yaml(work / "pair.yaml", record)
    return record


def run_backend_group_parallel(session: dict, backend: str, pairs: list[dict]) -> list[dict]:
    keep = bool((session.get("what") or {}).get("reuse_volumes"))
    session_id = session["id"]
    slugs = ", ".join(pair_slug(p) for p in pairs)
    print(f"backend {backend}: parallel {len(pairs)} replicas ({slugs})", flush=True)

    base_env = write_compose_env(session, pairs[0])
    if not keep:
        wipe_stack(base_env)

    compose_checked(
        ["up", "-d", "--build", *storage_stack_services([p["storage"] for p in pairs])],
        base_env,
        capture=False,
    )

    replicas: list[tuple[dict, str, int, Path]] = []
    for pair in pairs:
        slug = pair_slug(pair)
        container = parallel_container_name(session_id, pair["backend"], pair["storage"])
        host_port = parallel_host_port(pair["backend"], pair["storage"])
        env_file = write_compose_env(session, pair)
        internal = API_CONTAINER_PORT[pair["backend"]]
        lines = env_file.read_text(encoding="utf-8").splitlines()
        lines = [
            (f"GENERATOR_HTTP_URL=http://{container}:{internal}" if line.startswith("GENERATOR_HTTP_URL=") else line)
            for line in lines
        ]
        env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
        start_parallel_api(pair, env_file, container, host_port)
        wait_ready(f"http://127.0.0.1:{host_port}/readyz")
        skip_seed = resolve_skip_seed(session, pair, api_base=f"http://127.0.0.1:{host_port}")
        update_compose_seed_flag(env_file, skip_seed)
        if skip_seed:
            print(f"pair {slug}: archive seed skipped", flush=True)
        replicas.append((pair, container, host_port, env_file))

    prom_config = session_dir(session_id) / f"prometheus-{backend}.yml"
    prom_override = session_dir(session_id) / f"compose-prometheus-{backend}.yml"
    write_parallel_prometheus_config(
        prom_config,
        backend,
        [(pair, container) for pair, container, _, _ in replicas],
    )
    write_prometheus_compose_override(prom_override, prom_config)
    compose_checked(
        ["up", "-d", "--force-recreate", "prometheus"],
        base_env,
        capture=False,
        extra_files=[prom_override],
    )
    wait_ready("http://127.0.0.1:9090/-/ready")
    time.sleep(5)

    outputs: dict[str, tuple[str, int]] = {}
    with ThreadPoolExecutor(max_workers=len(replicas)) as pool:
        futures = {
            pool.submit(run_generator, session, pair, env_file): pair
            for pair, _, _, env_file in replicas
        }
        for future in as_completed(futures):
            pair = futures[future]
            slug = pair_slug(pair)
            try:
                outputs[slug] = future.result()
            except Exception as exc:
                outputs[slug] = (str(exc), 1)

    records: list[dict] = []
    dispatch = (session.get("what") or {}).get("dispatch") or "parallel-by-backend"
    for pair, container, host_port, env_file in replicas:
        slug = pair_slug(pair)
        skip_seed = resolve_skip_seed(session, pair, api_base=f"http://127.0.0.1:{host_port}")
        output, rc = outputs.get(slug, ("missing generator output", 1))
        meta_url = f"http://127.0.0.1:{host_port}/api/meta"
        record = finalize_pair_record(
            session,
            pair,
            env_file=env_file,
            skip_seed=skip_seed,
            generator_output=output,
            generator_rc=rc,
            meta_url=meta_url,
            api_container=container,
            dispatch=dispatch,
        )
        if record.get("status") != "completed":
            print(f"pair {slug}: failed: {record.get('error', 'unknown')}", file=sys.stderr)
        records.append(record)
        remove_container(container)

    if not keep:
        print(f"backend {backend}: cleanup stack", flush=True)
        wipe_stack(base_env)
    return records


def run_backend_group(session: dict, backend: str, pairs: list[dict]) -> list[dict]:
    keep = bool((session.get("what") or {}).get("reuse_volumes"))
    print(f"backend {backend}: up all DBs ({len(pairs)} storages)", flush=True)
    records: list[dict] = []
    for index, pair in enumerate(pairs):
        env_file = write_compose_env(session, pair)
        if index == 0:
            if not keep:
                wipe_stack(env_file)
            ensure_backend_stack(session, backend, env_file)
        else:
            recreate_api(pair["backend"], env_file)
        wait_ready(API_READY[pair["backend"]])
        if index > 0 or keep:
            time.sleep(5)
        records.append(measure_pair(session, pair, stack_ready=True))
    if not keep:
        env_file = write_compose_env(session, pairs[0])
        print(f"backend {backend}: cleanup stack", flush=True)
        wipe_stack(env_file)
    return records


def run_pair(session: dict, pair: dict) -> dict:
    dispatch = (session.get("what") or {}).get("dispatch") or "by-backend"
    if dispatch == "by-pair":
        return run_pair_isolated(session, pair)
    raise SessionError("run_pair() called in by-backend mode; use run_backend_group()")


def cmd_new(args: argparse.Namespace) -> int:
    if args.isolated_pairs:
        dispatch = "by-pair"
    elif args.dispatch:
        dispatch = args.dispatch
    else:
        dispatch = "parallel-by-backend"
    session = new_session(
        why=args.why,
        profile=args.profile,
        duration=args.duration,
        transport=args.transport,
        pairs=args.pairs,
        reuse_volumes=args.keep,
        archive_end=args.archive_end,
        dispatch=dispatch,
        skip_preflight=args.skip_preflight,
    )
    path = save_session(session)
    print(f"created {session['id']} ({path})")
    print(f"dispatch: {session['what']['dispatch']}  volume_set: {volume_set_for(session)}")
    print("pairs: " + ", ".join(pair_slug(p) for p in session["what"]["pairs"]))
    if args.run:
        return cmd_run(
            argparse.Namespace(
                id=session["id"],
                from_pair=None,
                only_pair=None,
                fail_fast=args.fail_fast,
                keep=args.keep,
                skip_preflight=args.skip_preflight,
            )
        )
    return 0


def _apply_run_flags(session: dict, args: argparse.Namespace) -> None:
    if getattr(args, "keep", False):
        session["what"]["reuse_volumes"] = True
        session["what"]["skip_seed"] = True
        session["what"].setdefault("archive_end", LAB_ARCHIVE_END)
    if getattr(args, "skip_preflight", False):
        session["what"]["skip_preflight"] = True


def _dispatch_pairs(session: dict, args: argparse.Namespace, pairs: list[dict]) -> int:
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
    selected = pairs[start_at:stop_at]
    dispatch = (session.get("what") or {}).get("dispatch") or "parallel-by-backend"
    failed = False

    if dispatch == "by-pair":
        for pair in selected:
            slug = pair_slug(pair)
            record = run_pair_isolated(session, pair)
            session["results"]["pairs"][slug] = record
            session["results"]["comparison"] = build_comparison(session)
            session["conclusions"] = draft_conclusions(session)
            save_session(session)
            write_comparison(session)
            if record.get("status") != "completed":
                failed = True
                if args.fail_fast:
                    raise SessionError(f"pair {slug} failed, stopping")
        return 1 if failed else 0

    run_group = run_backend_group_parallel if dispatch == "parallel-by-backend" else run_backend_group

    for backend, group in group_pairs_by_backend(selected):
        records = run_group(session, backend, group)
        for record in records:
            slug = pair_slug({"backend": record["backend"], "storage": record["storage"]})
            session["results"]["pairs"][slug] = record
            session["results"]["comparison"] = build_comparison(session)
            session["conclusions"] = draft_conclusions(session)
            save_session(session)
            write_comparison(session)
            if record.get("status") != "completed":
                failed = True
                if args.fail_fast:
                    raise SessionError(f"pair {slug} failed, stopping")
    return 1 if failed else 0


def cmd_run(args: argparse.Namespace) -> int:
    session_id = args.id or _latest_planned()
    session = load_session(session_id)
    pairs = [normalize_pair(p) for p in session["what"]["pairs"]]
    if not pairs:
        raise SessionError("session has no pairs")
    _apply_run_flags(session, args)
    session["status"] = "running"
    session["when"]["started_at"] = session["when"].get("started_at") or utcnow()
    session.setdefault("results", {}).setdefault("pairs", {})
    save_session(session)
    slugs = [pair_slug(p) for p in pairs]
    print(
        f"dispatch {session_id} mode={(session.get('what') or {}).get('dispatch')} "
        f"volume_set={volume_set_for(session)} "
        f"duration={session['what']['duration']} "
        f"profile={session['what']['load']['profile']} pairs={', '.join(slugs)}",
        flush=True,
    )
    failed = False
    try:
        if not session.get("what", {}).get("skip_preflight"):
            run_session_preflight(session)
        failed = bool(_dispatch_pairs(session, args, pairs))
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


def cmd_preflight(args: argparse.Namespace) -> int:
    if args.id:
        session = load_session(args.id)
    else:
        session = new_session(
            why="preflight-only",
            profile=args.profile,
            pairs=args.pairs,
            skip_preflight=False,
        )
    _apply_run_flags(session, args)
    run_session_preflight(session)
    return 0


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
        help="comma-separated backend:storage, e.g. go:timescaledb,rust:questdb",
    )
    new.add_argument("--run", action="store_true", help="dispatch immediately")
    new.add_argument("--fail-fast", action="store_true")
    new.add_argument("--keep", action="store_true", help="reuse volumes, skip wipe and archive seed")
    new.add_argument("--archive-end", help="UTC end of the existing archive (required for --keep query-mix)")
    new.add_argument(
        "--isolated-pairs",
        action="store_true",
        help="legacy dispatch: one pair at a time with full wipe between pairs",
    )
    new.add_argument(
        "--dispatch",
        choices=("parallel-by-backend", "by-backend", "by-pair"),
        help="parallel-by-backend: N API replicas per backend at once; by-backend: switch storage; by-pair: wipe each pair",
    )
    new.add_argument("--skip-preflight", action="store_true", help="do not run contract parity before load")
    new.set_defaults(func=cmd_new)

    run = sub.add_parser("run", help="dispatch pairs and record scorecard")
    run.add_argument("id", nargs="?", help="session id; defaults to the latest planned")
    run.add_argument("--from-pair", help="resume from this pair slug, e.g. rust-questdb")
    run.add_argument("--only-pair", help="run a single pair slug and keep the rest of the session")
    run.add_argument("--fail-fast", action="store_true")
    run.add_argument("--keep", action="store_true", help="reuse volumes, skip wipe and archive seed")
    run.add_argument("--skip-preflight", action="store_true", help="do not run contract parity before load")
    run.set_defaults(func=cmd_run)

    preflight = sub.add_parser("preflight", help="contract parity across backend x storage")
    preflight.add_argument("id", nargs="?", help="optional session id for volume_set/profile context")
    preflight.add_argument("--profile", default="query-mix", help="profile for volume_set selection")
    preflight.add_argument(
        "--pairs",
        help="comma-separated backend:storage list (default: session pairs or csharp × timescale/vm)",
    )
    preflight.add_argument("--keep", action="store_true", help="use data volume set without wipe")
    preflight.set_defaults(func=cmd_preflight)

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
