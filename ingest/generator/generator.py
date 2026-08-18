"""Synthetic time-series load for the Prism stand."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import random
import signal
import time
from datetime import datetime, timezone

import httpx
import nats


METRICS = ("cpu.usage", "mem.used", "disk.io", "net.rx", "net.tx")


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


async def publish_nats(url: str, subject: str, payload: bytes) -> None:
    nc = await nats.connect(url)
    try:
        await nc.publish(subject, payload)
        await nc.flush()
    finally:
        await nc.drain()


class Publisher:
    def __init__(self, target: str, http_url: str, nats_url: str, subject: str) -> None:
        self.target = target
        self.http_url = http_url.rstrip("/")
        self.nats_url = nats_url
        self.subject = subject
        self._http: httpx.AsyncClient | None = None
        self._nc = None

    async def start(self) -> None:
        if self.target == "http":
            self._http = httpx.AsyncClient(timeout=10.0)
            return
        self._nc = await nats.connect(self.nats_url, name="prism-generator")

    async def send(self, points: list[dict]) -> None:
        body = json.dumps({"points": points}).encode()
        if self.target == "http":
            assert self._http is not None
            resp = await self._http.post(f"{self.http_url}/v1/points", content=body, headers={"Content-Type": "application/json"})
            resp.raise_for_status()
            return
        assert self._nc is not None
        await self._nc.publish(self.subject, body)

    async def close(self) -> None:
        if self._http is not None:
            await self._http.aclose()
        if self._nc is not None:
            await self._nc.drain()


def point(device: str, metric: str, now: datetime) -> dict:
    phase = hash(device + metric) % 1000
    wave = 50 + 40 * math.sin((now.timestamp() + phase) / 15)
    noise = random.uniform(-4, 4)
    return {
        "ts": now.isoformat(),
        "metric": metric,
        "value": round(max(0.0, wave + noise), 4),
        "labels": {"host": device, "site": "lab"},
    }


async def run(args: argparse.Namespace) -> None:
    pub = Publisher(args.target, args.http_url, args.nats_url, args.subject)
    await pub.start()
    devices = [f"dev-{i:03d}" for i in range(args.devices)]
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    batch_size = max(args.batch, 1)
    interval = batch_size / args.rate if args.rate > 0 else 1
    sent = 0
    started = time.monotonic()
    deadline = started + args.duration if args.duration > 0 else None

    print(
        f"generator target={args.target} rate={args.rate}/s devices={args.devices} "
        f"batch={batch_size} duration={args.duration or 'inf'}",
        flush=True,
    )
    try:
        while not stop.is_set():
            if deadline is not None and time.monotonic() >= deadline:
                break
            tick = datetime.now(timezone.utc)
            batch = [
                point(random.choice(devices), random.choice(METRICS), tick)
                for _ in range(batch_size)
            ]
            await pub.send(batch)
            sent += len(batch)
            await asyncio.sleep(interval)
    finally:
        elapsed = max(time.monotonic() - started, 1e-6)
        print(f"generator done sent={sent} elapsed={elapsed:.1f}s rate={sent / elapsed:.1f}/s", flush=True)
        await pub.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Prism time-series load generator")
    p.add_argument("--target", default=env("TARGET", "nats"), choices=("nats", "http"))
    p.add_argument("--http-url", default=env("HTTP_URL", "http://localhost:8081"))
    p.add_argument("--nats-url", default=env("NATS_URL", "nats://localhost:4222"))
    p.add_argument("--subject", default=env("NATS_SUBJECT", "prism.points"))
    p.add_argument("--rate", type=float, default=float(env("RATE", "2000")))
    p.add_argument("--devices", type=int, default=int(env("DEVICES", "50")))
    p.add_argument("--batch", type=int, default=int(env("BATCH", "100")))
    p.add_argument("--duration", type=float, default=float(env("DURATION", "0")))
    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(run(parse_args()))
