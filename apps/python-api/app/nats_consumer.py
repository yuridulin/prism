import asyncio
import json
import logging
from datetime import datetime, timezone

import nats

from app.metrics import observe_backend, track
from app.models import Sample, WriteRequest
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
            if "samples" in payload:
                samples = WriteRequest.model_validate(payload).samples
            else:
                samples = [Sample.model_validate(payload)]
            if not samples:
                return
            now = datetime.now(timezone.utc)
            for s in samples:
                if s.ts.tzinfo is None:
                    s.ts = s.ts.replace(tzinfo=timezone.utc)
                if s.ts == datetime(1970, 1, 1, tzinfo=timezone.utc):
                    s.ts = now
            with track() as elapsed:
                try:
                    await store.write(samples)
                except Exception as exc:
                    observe_backend(store.name, "write", "nats", 0, elapsed(), exc)
                    raise
            observe_backend(store.name, "write", "nats", len(samples), elapsed())
        except Exception as exc:
            log.warning("nats write failed: %s", exc)

    await nc.subscribe(subject, queue="prism-python", cb=handler)
    try:
        await asyncio.Event().wait()
    finally:
        await nc.drain()
