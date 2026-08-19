"""Profile-driven load generator for the Prism stand."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import random
import signal
import time
from datetime import datetime, timedelta, timezone

from profile import Profile, ProfileError, load_profile, list_profiles, pick_mix

QUALITY_GOOD = 192
QUALITY_BAD = 0
QUALITY_UNCERTAIN = 64


def env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


class Publisher:
    def __init__(self, transport: str, http_url: str, nats_url: str, subject: str) -> None:
        self.transport = transport
        self.http_url = http_url.rstrip("/")
        self.nats_url = nats_url
        self.subject = subject
        self._http: httpx.AsyncClient | None = None
        self._nc = None

    async def start(self) -> None:
        import httpx

        self._http = httpx.AsyncClient(timeout=15.0)
        if self.transport == "nats":
            import nats

            self._nc = await nats.connect(self.nats_url, name="prism-generator")

    async def write(self, samples: list[dict]) -> None:
        body = json.dumps({"samples": samples}).encode()
        if self.transport == "http":
            assert self._http is not None
            resp = await self._http.post(
                f"{self.http_url}/v1/write",
                content=body,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            return
        assert self._nc is not None
        await self._nc.publish(self.subject, body)

    async def read(self, payload: dict) -> None:
        assert self._http is not None
        resp = await self._http.post(f"{self.http_url}/v1/read", json=payload)
        resp.raise_for_status()

    async def close(self) -> None:
        if self._http is not None:
            await self._http.aclose()
        if self._nc is not None:
            await self._nc.drain()


def pick_tag(profile: Profile, rng: random.Random) -> int:
    return profile.ingest.tag_start + rng.randrange(profile.ingest.tag_count)


def pick_quality(profile: Profile, rng: random.Random) -> int:
    if rng.random() < profile.ingest.good_ratio:
        return QUALITY_GOOD
    return QUALITY_BAD if rng.random() < 0.5 else QUALITY_UNCERTAIN


def make_sample(profile: Profile, rng: random.Random, now: datetime) -> dict:
    ts = now
    ingest = profile.ingest
    if ingest.out_of_order > 0 and rng.random() < ingest.out_of_order:
        lag = rng.randint(1, max(ingest.late_ms, 1))
        ts = now - timedelta(milliseconds=lag)
    tag_id = pick_tag(profile, rng)
    wave = 50 + 40 * math.sin((now.timestamp() + tag_id) / 15)
    return {
        "ts": ts.isoformat().replace("+00:00", "Z"),
        "tag_id": tag_id,
        "value": round(max(0.0, wave + rng.uniform(-4, 4)), 4),
        "quality": pick_quality(profile, rng),
    }


def parse_window(raw: str) -> timedelta:
    units = {"ms": 0.001, "s": 1, "m": 60, "h": 3600}
    for suffix, mul in units.items():
        if raw.endswith(suffix):
            return timedelta(seconds=float(raw[: -len(suffix)]) * mul)
    raise ProfileError(f"invalid window {raw!r}")


async def ingest_worker(profile: Profile, pub: Publisher, stop: asyncio.Event, stats: dict) -> None:
    spec = profile.ingest
    interval = spec.batch / spec.rate if spec.rate > 0 else 1.0
    rng = random.Random()
    while not stop.is_set():
        tick = datetime.now(timezone.utc)
        batch = [make_sample(profile, rng, tick) for _ in range(spec.batch)]
        try:
            await pub.write(batch)
            stats["written"] += len(batch)
        except Exception as exc:
            stats["write_errors"] += 1
            print(f"ingest error: {exc}", flush=True)
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            pass


async def query_worker(profile: Profile, pub: Publisher, stop: asyncio.Event, stats: dict) -> None:
    spec = profile.query
    interval = 1.0 / spec.rate if spec.rate > 0 else 1.0
    rng = random.Random()
    while not stop.is_set():
        item = pick_mix(spec.mix, rng)
        tag_ids = [pick_tag(profile, rng)]
        now = datetime.now(timezone.utc)
        try:
            if item.op == "locf":
                await pub.read({"mode": "locf", "tag_ids": tag_ids, "at": now.isoformat().replace("+00:00", "Z")})
            else:
                start = now - parse_window(item.window)
                payload = {
                    "mode": item.op,
                    "tag_ids": tag_ids,
                    "from": start.isoformat().replace("+00:00", "Z"),
                    "to": now.isoformat().replace("+00:00", "Z"),
                }
                if item.op == "sample":
                    payload["step"] = item.step
                await pub.read(payload)
            stats["queries"] += 1
        except Exception as exc:
            stats["query_errors"] += 1
            print(f"query error: {exc}", flush=True)
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            pass


async def run(profile: Profile, args: argparse.Namespace) -> None:
    pub = Publisher(args.transport, args.http_url, args.nats_url, args.subject)
    await pub.start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    stats = {"written": 0, "write_errors": 0, "queries": 0, "query_errors": 0}
    started = time.monotonic()
    if args.duration > 0:
        loop.call_later(args.duration, stop.set)

    tasks: list[asyncio.Task] = []
    if profile.ingest.enabled and profile.ingest.rate > 0:
        for _ in range(profile.ingest.workers):
            tasks.append(asyncio.create_task(ingest_worker(profile, pub, stop, stats)))
    if profile.query.enabled and profile.query.rate > 0:
        tasks.append(asyncio.create_task(query_worker(profile, pub, stop, stats)))
    if not tasks:
        raise ProfileError("nothing to run: enable ingest or query with a non-zero rate")

    print(
        f"generator profile={profile.name} transport={args.transport} "
        f"ingest={profile.ingest.rate}/s query={profile.query.rate}/s "
        f"tags={profile.sample_space_size()} duration={args.duration or 'inf'}",
        flush=True,
    )
    try:
        await stop.wait()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        elapsed = max(time.monotonic() - started, 1e-6)
        print(
            f"generator done profile={profile.name} written={stats['written']} "
            f"queries={stats['queries']} write_errors={stats['write_errors']} "
            f"query_errors={stats['query_errors']} elapsed={elapsed:.1f}s "
            f"ingest_rate={stats['written'] / elapsed:.1f}/s",
            flush=True,
        )
        await pub.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prism profile-driven load generator")
    parser.add_argument("--profile", default=env("PROFILE", "iot-steady"))
    parser.add_argument("--profiles-dir", default=env("PROFILES_DIR"))
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--transport", default=env("TARGET") or "", choices=("", "nats", "http"))
    parser.add_argument("--http-url", default=env("HTTP_URL", "http://localhost:8081"))
    parser.add_argument("--nats-url", default=env("NATS_URL", "nats://localhost:4222"))
    parser.add_argument("--subject", default=env("NATS_SUBJECT", "prism.samples"))
    parser.add_argument("--duration", type=float, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = args.profiles_dir
    if args.list:
        print("\n".join(list_profiles(root)))
        return
    profile = load_profile(args.profile, root)
    if not args.transport:
        args.transport = profile.transport
    if args.duration is None:
        override = env("DURATION")
        args.duration = float(override) if override is not None else profile.duration
    asyncio.run(run(profile, args))


if __name__ == "__main__":
    main()
