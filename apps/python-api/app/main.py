import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.config import SUPPORTED, settings
from app.metrics import INGEST_ERRORS, INGEST_POINTS, QUERY_DURATION, QUERY_ERRORS
from app.models import Meta, Point, QueryResult, WriteRequest
from app.nats_consumer import run_consumer
from app.store import create_store
from app.store.base import Store

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("prism")

store: Store
consumer_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global store, consumer_task
    store = create_store()
    await store.ping()
    log.info("python-api storage=%s", store.name)
    try:
        consumer_task = asyncio.create_task(run_consumer(settings.nats_url, settings.nats_subject, store))
    except Exception as exc:
        log.warning("nats unavailable, HTTP-only mode: %s", exc)
        consumer_task = None
    yield
    if consumer_task is not None:
        consumer_task.cancel()
    await store.close()


app = FastAPI(title="Prism Python API", lifespan=lifespan)


@app.get("/healthz", response_class=PlainTextResponse)
async def healthz() -> str:
    return "ok"


@app.get("/readyz", response_class=PlainTextResponse)
async def readyz() -> str:
    try:
        await store.ping()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return "ready"


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/v1/meta", response_model=Meta)
async def meta() -> Meta:
    return Meta(backend="python", storage=store.name, storages=SUPPORTED)


@app.post("/v1/points", status_code=204)
async def write_points(req: WriteRequest) -> Response:
    if not req.points:
        raise HTTPException(status_code=400, detail="points is required")
    now = datetime.now(timezone.utc)
    for p in req.points:
        if p.ts.tzinfo is None:
            p.ts = p.ts.replace(tzinfo=timezone.utc)
        if p.ts.timestamp() == 0:
            p.ts = now
    try:
        await store.write(req.points)
    except Exception as exc:
        INGEST_ERRORS.labels("python", store.name).inc()
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    INGEST_POINTS.labels("python", store.name).inc(len(req.points))
    return Response(status_code=204)


@app.get("/v1/query", response_model=QueryResult)
async def query_points(
    metric: str,
    from_: datetime = Query(alias="from"),
    to: datetime = Query(),
    step: str = "1m",
    agg: str = "avg",
    labels: str | None = None,
) -> QueryResult:
    if agg not in {"avg", "min", "max", "sum", "count"}:
        raise HTTPException(status_code=400, detail="invalid agg")
    try:
        step_td = _parse_step(step)
        parsed_labels = json.loads(labels) if labels else {}
    except (ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    with QUERY_DURATION.labels("python", store.name).time():
        try:
            return await store.query(metric, from_, to, step_td, agg, parsed_labels)
        except Exception as exc:
            QUERY_ERRORS.labels("python", store.name).inc()
            raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/v1/latest", response_model=Point)
async def latest(metric: str, labels: str | None = None) -> Point:
    try:
        parsed_labels = json.loads(labels) if labels else {}
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="labels must be a JSON object") from exc
    with QUERY_DURATION.labels("python", store.name).time():
        try:
            point = await store.latest(metric, parsed_labels)
        except Exception as exc:
            QUERY_ERRORS.labels("python", store.name).inc()
            raise HTTPException(status_code=500, detail=str(exc)) from exc
    if point is None:
        raise HTTPException(status_code=404, detail="not found")
    return point


def _parse_step(raw: str) -> timedelta:
    try:
        return timedelta(seconds=int(raw.rstrip("s"))) if raw.endswith("s") and raw[:-1].isdigit() else _parse_go_duration(raw)
    except ValueError as exc:
        raise ValueError("invalid step") from exc


def _parse_go_duration(raw: str) -> timedelta:
    units = {"ms": 0.001, "s": 1, "m": 60, "h": 3600}
    for suffix, mul in units.items():
        if raw.endswith(suffix):
            return timedelta(seconds=float(raw[: -len(suffix)]) * mul)
    raise ValueError("invalid step")
