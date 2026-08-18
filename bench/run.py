"""Run a Prism load profile through the generator or k6."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ingest" / "generator"))

from profile import list_profiles, load_profile, profiles_dir, to_k6_env  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a Prism load profile")
    parser.add_argument("--profile", default=os.getenv("PROFILE", "iot-steady"))
    parser.add_argument("--mode", choices=("generator", "k6", "dump"), default="generator")
    parser.add_argument("--base-url", default=os.getenv("BASE_URL", "http://localhost:8081"))
    parser.add_argument("--transport", default="")
    parser.add_argument("--duration", default="")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()

    if args.list:
        print("\n".join(list_profiles()))
        return 0

    profile = load_profile(args.profile)
    if args.mode == "dump":
        print(json.dumps(to_k6_env(profile), indent=2))
        return 0

    if args.mode == "k6":
        env = os.environ.copy()
        env["BASE_URL"] = args.base_url
        env["PROFILE_JSON"] = json.dumps(to_k6_env(profile))
        if args.duration:
            env["DURATION"] = args.duration if args.duration.endswith("s") else f"{args.duration}s"
        script = ROOT / "bench" / "k6" / "load.js"
        return subprocess.call(["k6", "run", str(script)], env=env)

    cmd = [
        sys.executable,
        str(ROOT / "ingest" / "generator" / "generator.py"),
        "--profile",
        args.profile,
        "--http-url",
        args.base_url,
        "--profiles-dir",
        str(profiles_dir()),
    ]
    if args.transport:
        cmd.extend(["--transport", args.transport])
    if args.duration:
        cmd.extend(["--duration", args.duration])
    return subprocess.call(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
