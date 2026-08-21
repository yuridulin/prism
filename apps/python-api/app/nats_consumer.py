import asyncio
import json
import logging
from datetime import datetime, timezone

import nats

from app.metrics import observe_backend, track
from app.models import parse_write_payload
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
            items = parse_write_payload(payload)
            if not items:
                return
            now = datetime.now(timezone.utc)
            samples = [item.to_sample(now) for item in items]
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
