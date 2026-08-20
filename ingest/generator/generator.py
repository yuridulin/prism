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

from profile import (
    ArchiveSpec,
    Profile,
    ProfileError,
    QueryCall,
    archive_bounds,
    archive_sample_count,
    build_query_grid,
    load_profile,
    list_profiles,
    parse_duration,
    pick_mix,
    rfc3339,
)

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

        self._http = httpx.AsyncClient(timeout=120.0)
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

    async def write_http(self, samples: list[dict]) -> None:
        assert self._http is not None
        resp = await self._http.post(
            f"{self.http_url}/v1/write",
            json={"samples": samples},
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()

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


def sample_value(tag_id: int, ts: datetime) -> float:
    wave = 50 + 40 * math.sin((ts.timestamp() + tag_id) / 15)
    return round(max(0.0, wave), 4)


def make_sample(profile: Profile, rng: random.Random, now: datetime) -> dict:
    ts = now
    ingest = profile.ingest
    if ingest.out_of_order > 0 and rng.random() < ingest.out_of_order:
        lag = rng.randint(1, max(ingest.late_ms, 1))
        ts = now - timedelta(milliseconds=lag)
    tag_id = pick_tag(profile, rng)
    return {
        "ts": rfc3339(ts),
        "tag_id": tag_id,
        "value": round(max(0.0, sample_value(tag_id, ts) + rng.uniform(-4, 4)), 4),
        "quality": pick_quality(profile, rng),
    }


def make_archive_sample(tag_id: int, ts: datetime) -> dict:
    return {
        "ts": rfc3339(ts),
        "tag_id": tag_id,
        "value": sample_value(tag_id, ts),
        "quality": QUALITY_GOOD,
    }


def parse_archive_end(raw: str | None) -> datetime:
    if raw:
        text = raw.strip().replace("Z", "+00:00")
        return datetime.fromisoformat(text).astimezone(timezone.utc)
    return datetime.now(timezone.utc)


def first_tick(origin: datetime, period: timedelta, slice_start: datetime) -> datetime:
    elapsed = (slice_start - origin).total_seconds()
    step = period.total_seconds()
    if elapsed <= 0:
        return origin
    steps = math.ceil(elapsed / step - 1e-9)
    return origin + timedelta(seconds=steps * step)


async def ingest_worker(profile: Profile, pub: Publisher, stop: asyncio.Event, stats: dict) -> None:
    spec = profile.ingest
    interval = spec.batch / spec.rate if spec.rate > 0 else 1.0
    rng = random.Random()
    while not stop.is_set():
        tick = datetime.now(timezone.utc)
        started = time.monotonic()
        batch = [make_sample(profile, rng, tick) for _ in range(spec.batch)]
        try:
            await pub.write(batch)
            stats["written"] += len(batch)
        except Exception as exc:
            stats["write_errors"] += 1
            print(f"ingest error: {exc}", flush=True)
        remaining = interval - (time.monotonic() - started)
        if remaining <= 0:
            continue
        try:
            await asyncio.wait_for(stop.wait(), timeout=remaining)
        except TimeoutError:
            pass


async def query_worker_live(profile: Profile, pub: Publisher, stop: asyncio.Event, stats: dict) -> None:
    spec = profile.query
    interval = spec.workers / spec.rate if spec.rate > 0 else 1.0
    rng = random.Random()
    while not stop.is_set():
        item = pick_mix(spec.mix, rng)
        tag_ids = [pick_tag(profile, rng)]
        now = datetime.now(timezone.utc)
        try:
            if item.op == "locf":
                await pub.read({"mode": "locf", "tag_ids": tag_ids, "at": rfc3339(now)})
            else:
                start = now - parse_duration(item.window)
                payload = {
                    "mode": item.op,
                    "tag_ids": tag_ids,
                    "from": rfc3339(start),
                    "to": rfc3339(now),
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


async def query_worker_grid(
    grid: list[QueryCall],
    cursor: dict,
    lock: asyncio.Lock,
    pub: Publisher,
    stop: asyncio.Event,
    stats: dict,
    interval: float,
) -> None:
    while not stop.is_set():
        async with lock:
            call = grid[cursor["i"] % len(grid)]
            cursor["i"] += 1
        try:
            await pub.read(call.payload())
            stats["queries"] += 1
        except Exception as exc:
            stats["query_errors"] += 1
            print(f"query error {call.label}: {exc}", flush=True)
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            pass


async def flush_seed(pub: Publisher, batch: list[dict], stats: dict, lock: asyncio.Lock, stop: asyncio.Event) -> None:
    last_exc: Exception | None = None
    for attempt in range(5):
        if stop.is_set():
            return
        try:
            await pub.write_http(batch)
            async with lock:
                stats["written"] += len(batch)
            return
        except Exception as exc:
            last_exc = exc
            await asyncio.sleep(0.4 * (attempt + 1))
    async with lock:
        stats["write_errors"] += 1
    print(f"ingest error: {last_exc}", flush=True)


async def seed_worker(
    archive: ArchiveSpec,
    origin: datetime,
    slice_start: datetime,
    slice_end: datetime,
    include_end: bool,
    pub: Publisher,
    stats: dict,
    lock: asyncio.Lock,
    stop: asyncio.Event,
) -> None:
    states = []
    for cls in archive.tags:
        period = parse_duration(cls.period)
        ts = first_tick(origin, period, slice_start)
        states.append([ts, period, cls.ids()])
    batch: list[dict] = []
    while not stop.is_set():
        upcoming = []
        for i, state in enumerate(states):
            ts = state[0]
            if include_end:
                if ts <= slice_end:
                    upcoming.append((i, ts))
            elif ts < slice_end:
                upcoming.append((i, ts))
        if not upcoming:
            break
        i, ts = min(upcoming, key=lambda item: item[1])
        _, period, ids = states[i]
        for tag_id in ids:
            batch.append(make_archive_sample(tag_id, ts))
            if len(batch) >= archive.batch:
                await flush_seed(pub, batch, stats, lock, stop)
                batch = []
        states[i][0] = ts + period
    if batch and not stop.is_set():
        await flush_seed(pub, batch, stats, lock, stop)


async def seed_archive(
    profile: Profile,
    pub: Publisher,
    stats: dict,
    stop: asyncio.Event,
    archive_end: datetime,
) -> float:
    archive = profile.archive
    start, end = archive_bounds(archive.span, archive_end)
    expected = archive_sample_count(archive, start, end)
    workers = archive.workers
    print(
        f"generator seed profile={profile.name} span={archive.span} "
        f"from={rfc3339(start)} to={rfc3339(end)} samples={expected} "
        f"workers={workers} batch={archive.batch}",
        flush=True,
    )
    lock = asyncio.Lock()
    span = end - start
    tasks = []
    for wid in range(workers):
        slice_start = start + span * wid / workers
        slice_end = start + span * (wid + 1) / workers
        last = wid == workers - 1
        if last:
            slice_end = end
        tasks.append(
            asyncio.create_task(
                seed_worker(archive, start, slice_start, slice_end, last, pub, stats, lock, stop)
            )
        )
    started = time.monotonic()
    progress = asyncio.create_task(_seed_progress(expected, stats, stop, started))
    await asyncio.gather(*tasks)
    progress.cancel()
    elapsed = max(time.monotonic() - started, 1e-6)
    print(
        f"generator seed done profile={profile.name} written={stats['written']} "
        f"errors={stats['write_errors']} elapsed={elapsed:.1f}s "
        f"rate={stats['written'] / elapsed:.1f}/s",
        flush=True,
    )
    return elapsed


async def _seed_progress(expected: int, stats: dict, stop: asyncio.Event, started: float) -> None:
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=10.0)
            return
        except TimeoutError:
            elapsed = max(time.monotonic() - started, 1e-6)
            written = stats["written"]
            print(
                f"generator seed progress written={written}/{expected} "
                f"errors={stats['write_errors']} elapsed={elapsed:.1f}s "
                f"rate={written / elapsed:.1f}/s",
                flush=True,
            )


async def run_query_grid(
    profile: Profile,
    pub: Publisher,
    stats: dict,
    stop: asyncio.Event,
    archive_end: datetime,
    duration: float,
) -> None:
    start, end = archive_bounds(profile.archive.span, archive_end)
    grid = build_query_grid(profile, start, end)
    rng = random.Random(profile.query.rng_seed)
    rng.shuffle(grid)
    workers = profile.query.workers
    interval = workers / profile.query.rate if profile.query.rate > 0 else 1.0
    print(
        f"generator query grid={len(grid)} rate={profile.query.rate}/s "
        f"workers={workers} duration={duration or 'inf'}",
        flush=True,
    )
    lock = asyncio.Lock()
    cursor = {"i": 0}
    tasks = [
        asyncio.create_task(query_worker_grid(grid, cursor, lock, pub, stop, stats, interval))
        for _ in range(workers)
    ]
    if duration > 0:
        loop = asyncio.get_running_loop()
        loop.call_later(duration, stop.set)
    await stop.wait()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


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
    seed_elapsed = 0.0
    archive_end = parse_archive_end(args.archive_end)
    do_seed = bool(profile.archive.enabled and args.seed)

    try:
        if do_seed:
            seed_elapsed = await seed_archive(profile, pub, stats, stop, archive_end)
            if stop.is_set():
                raise ProfileError("seed interrupted")
            if stats["write_errors"]:
                raise ProfileError(f"archive seed had {stats['write_errors']} write errors")
            if stats["written"] == 0:
                raise ProfileError("archive seed wrote nothing")
        elif profile.archive.enabled:
            start, end = archive_bounds(profile.archive.span, archive_end)
            print(
                f"generator skip seed profile={profile.name} span={profile.archive.span} "
                f"from={rfc3339(start)} to={rfc3339(end)}",
                flush=True,
            )

        tasks: list[asyncio.Task] = []
        query_stop = asyncio.Event()

        def halt() -> None:
            stop.set()
            query_stop.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, halt)
            except NotImplementedError:
                pass

        live_duration = args.duration
        if profile.archive.enabled:
            live_duration = 0
        elif args.duration > 0:
            loop.call_later(args.duration, halt)

        if profile.ingest.enabled and profile.ingest.rate > 0:
            for _ in range(profile.ingest.workers):
                tasks.append(asyncio.create_task(ingest_worker(profile, pub, query_stop, stats)))
        if profile.query.enabled and profile.query.rate > 0:
            if profile.archive.enabled:
                await run_query_grid(profile, pub, stats, query_stop, archive_end, args.duration)
            else:
                for _ in range(profile.query.workers):
                    tasks.append(asyncio.create_task(query_worker_live(profile, pub, query_stop, stats)))
        elif not tasks and not profile.archive.enabled:
            raise ProfileError("nothing to run: enable ingest, archive or query with a non-zero rate")

        if tasks:
            print(
                f"generator profile={profile.name} transport={args.transport} "
                f"ingest={profile.ingest.rate}/s query={profile.query.rate}/s "
                f"tags={profile.sample_space_size()} duration={live_duration or args.duration or 'inf'}",
                flush=True,
            )
            await query_stop.wait()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        elif not profile.archive.enabled:
            raise ProfileError("nothing to run: enable ingest, archive or query with a non-zero rate")
    finally:
        elapsed = max(time.monotonic() - started, 1e-6)
        query_elapsed = max(elapsed - seed_elapsed, 0.0)
        ingest_rate = 0.0 if profile.archive.enabled else stats["written"] / elapsed
        print(
            f"generator done profile={profile.name} written={stats['written']} "
            f"queries={stats['queries']} write_errors={stats['write_errors']} "
            f"query_errors={stats['query_errors']} elapsed={elapsed:.1f}s "
            f"ingest_rate={ingest_rate:.1f}/s seed_elapsed={seed_elapsed:.1f}s "
            f"query_elapsed={query_elapsed:.1f}s",
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
    parser.add_argument("--archive-end", default=env("ARCHIVE_END"))
    parser.add_argument("--seed", dest="seed", action="store_true")
    parser.add_argument("--no-seed", dest="seed", action="store_false")
    seed_env = (env("ARCHIVE_SEED") or "1").strip().lower()
    parser.set_defaults(seed=seed_env not in {"0", "false", "no", "off"})
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
