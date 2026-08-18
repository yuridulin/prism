import asyncio
import json
import logging
from datetime import datetime, timezone

import nats

from app.metrics import observe_backend, track
from app.models import Point, WriteRequest
from app.store.base import Store

log = logging.getLogger("prism.nats")


async def run_consumer(url: str, subject: str, store: Store) -> None:
    last_err: Exception | None = None
    for _ in range(30):
        try:
            nc = await nats.connect(url, name="prism-python-api", max_reconnect_attempts=-1)
            break
        except Exception as exc:
            last_err = exc
            await asyncio.sleep(1)
    else:
        raise RuntimeError(f"nats connect failed: {last_err}")
    log.info("nats subscribed subject=%s queue=prism-python", subject)

    async def handler(msg) -> None:
        try:
            payload = json.loads(msg.data.decode())
            if "points" in payload:
                points = WriteRequest.model_validate(payload).points
            else:
                points = [Point.model_validate(payload)]
            if not points:
                return
            now = datetime.now(timezone.utc)
            for p in points:
                if p.ts.tzinfo is None:
                    p.ts = p.ts.replace(tzinfo=timezone.utc)
                if p.ts == datetime(1970, 1, 1, tzinfo=timezone.utc):
                    p.ts = now
            with track() as elapsed:
                try:
                    await store.write(points)
                except Exception as exc:
                    observe_backend(store.name, "write", "nats", 0, elapsed(), exc)
                    raise
            observe_backend(store.name, "write", "nats", len(points), elapsed())
        except Exception as exc:
            log.warning("nats write failed: %s", exc)

    await nc.subscribe(subject, queue="prism-python", cb=handler)
    try:
        await asyncio.Event().wait()
    finally:
        await nc.drain()
