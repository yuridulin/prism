"""Seed the same query-mix archive into every storage, then leave APIs mapped for lab work."""

from __future__ import annotations

import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV = ROOT / "sessions" / "query-lab.env"
COMPOSE = [
    "docker",
    "compose",
    "-f",
    "docker-compose.yml",
    "-f",
    "docker-compose.session.yml",
    "--env-file",
    str(ENV),
]
STORAGES = ("timescaledb", "clickhouse", "questdb", "influxdb", "victoriametrics")
API_READY = {
    "go": "http://127.0.0.1:8081/readyz",
    "python": "http://127.0.0.1:8082/readyz",
    "csharp": "http://127.0.0.1:8083/readyz",
    "rust": "http://127.0.0.1:8084/readyz",
}


def run(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    print("+", " ".join(args), flush=True)
    proc = subprocess.run(args, cwd=ROOT, check=False)
    if check and proc.returncode != 0:
        raise SystemExit(f"command failed ({proc.returncode}): {' '.join(args)}")
    return proc


def set_go_storage(name: str) -> None:
    lines = []
    for line in ENV.read_text(encoding="utf-8").splitlines():
        if line.startswith("GO_API_STORAGE="):
            lines.append(f"GO_API_STORAGE={name}")
        elif line.startswith("PRISM_STORAGE="):
            lines.append(f"PRISM_STORAGE={name}")
        else:
            lines.append(line)
    ENV.write_text("\n".join(lines) + "\n", encoding="utf-8")


def wait_ready(url: str, timeout: int = 240) -> None:
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                if resp.status == 200:
                    print(f"ready {url}", flush=True)
                    return
                last = str(resp.status)
        except Exception as exc:
            last = str(exc)
        time.sleep(3)
    raise SystemExit(f"not ready after {timeout}s ({url}: {last})")


def main() -> int:
    run(COMPOSE + ["up", "-d", "--build", "nats", "prometheus", "timescaledb", "clickhouse", "questdb", "influxdb", "victoriametrics"])
    for storage in STORAGES:
        print(f"=== seed {storage} ===", flush=True)
        set_go_storage(storage)
        run(COMPOSE + ["up", "-d", "--build", "--force-recreate", "go-api"])
        wait_ready(API_READY["go"])
        proc = run(COMPOSE + ["--profile", "load", "run", "--rm", "--no-deps", "generator"], check=False)
        if proc.returncode != 0:
            raise SystemExit(f"generator failed for {storage}")
    print("=== restore API map ===", flush=True)
    set_go_storage("timescaledb")
    run(COMPOSE + ["up", "-d", "--build", "--force-recreate", "go-api", "python-api", "csharp-api", "rust-api"])
    for url in API_READY.values():
        wait_ready(url)
    print("lab ready", flush=True)
    print("  go-api:8081 -> timescaledb", flush=True)
    print("  python-api:8082 -> clickhouse", flush=True)
    print("  csharp-api:8083 -> influxdb", flush=True)
    print("  rust-api:8084 -> questdb", flush=True)
    print("  victoriametrics:8428 (native; no dedicated API)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
