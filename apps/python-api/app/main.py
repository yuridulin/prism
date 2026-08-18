import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import PlainTextResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.config import SUPPORTED, settings
from app.errors import http_error_handler, validation_error_handler
from app.metrics import observe_api, observe_backend, track
from app.models import CONTRACT, OPS, LatestRequest, Meta, Point, QueryRequest, QueryResult, WriteRequest, WriteResponse
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
app.add_exception_handler(StarletteHTTPException, http_error_handler)
app.add_exception_handler(RequestValidationError, validation_error_handler)


@app.middleware("http")
async def api_metrics(request: Request, call_next):
    path = request.url.path
    if path in {"/metrics", "/healthz", "/readyz"}:
        return await call_next(request)
    with track() as elapsed:
        response = await call_next(request)
    route = path.strip("/").replace("/", "_") or "root"
    observe_api(store.name, route, request.method, str(response.status_code), elapsed())
    return response


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
async def metrics_endpoint() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/v1/meta", response_model=Meta)
async def meta() -> Meta:
    return Meta(backend="python", storage=store.name, storages=SUPPORTED, contract=CONTRACT, ops=OPS)


@app.post("/v1/points", response_model=WriteResponse)
async def write_points(req: WriteRequest) -> WriteResponse:
    if not req.points:
        raise HTTPException(status_code=400, detail="points is required")
    now = datetime.now(timezone.utc)
    for p in req.points:
        if p.ts.tzinfo is None:
            p.ts = p.ts.replace(tzinfo=timezone.utc)
        if p.ts.timestamp() == 0:
            p.ts = now
    with track() as elapsed:
        try:
            await store.write(req.points)
        except Exception as exc:
            observe_backend(store.name, "write", "http", 0, elapsed(), exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc
    observe_backend(store.name, "write", "http", len(req.points), elapsed())
    return WriteResponse(written=len(req.points))


@app.post("/v1/query", response_model=QueryResult)
async def query_post(req: QueryRequest) -> QueryResult:
    return await _query(req)


@app.get("/v1/query", response_model=QueryResult)
async def query_get(
    metric: str,
    from_: datetime = Query(alias="from"),
    to: datetime = Query(),
    step: str = "1m",
    agg: str = "avg",
    labels: str | None = None,
) -> QueryResult:
    parsed = _parse_labels(labels)
    return await _query(QueryRequest.model_validate({
        "metric": metric,
        "from": from_,
        "to": to,
        "step": step,
        "agg": agg,
        "labels": parsed,
    }))


@app.post("/v1/latest", response_model=Point)
async def latest_post(req: LatestRequest) -> Point:
    return await _latest(req)


@app.get("/v1/latest", response_model=Point)
async def latest_get(metric: str, labels: str | None = None) -> Point:
    return await _latest(LatestRequest(metric=metric, labels=_parse_labels(labels)))


async def _query(req: QueryRequest) -> QueryResult:
    if req.agg not in {"avg", "min", "max", "sum", "count"}:
        raise HTTPException(status_code=400, detail="invalid agg")
    try:
        step_td = _parse_step(req.step)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    with track() as elapsed:
        try:
            result = await store.query(req.metric, req.from_, req.to, step_td, req.agg, req.labels or {})
        except Exception as exc:
            observe_backend(store.name, "query", "http", 0, elapsed(), exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc
    result.step = req.step or "1m"
    observe_backend(store.name, "query", "http", len(result.points), elapsed())
    return result


async def _latest(req: LatestRequest) -> Point:
    with track() as elapsed:
        try:
            point = await store.latest(req.metric, req.labels or {})
        except Exception as exc:
            observe_backend(store.name, "latest", "http", 0, elapsed(), exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc
    if point is None:
        observe_backend(store.name, "latest", "http", 0, elapsed(), not_found=True)
        raise HTTPException(status_code=404, detail="not found")
    observe_backend(store.name, "latest", "http", 1, elapsed())
    return point


def _parse_labels(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="labels must be a JSON object") from exc
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail="labels must be a JSON object")
    return {str(k): str(v) for k, v in parsed.items()}


def _parse_step(raw: str) -> timedelta:
    units = {"ms": 0.001, "s": 1, "m": 60, "h": 3600}
    for suffix, mul in units.items():
        if raw.endswith(suffix):
            return timedelta(seconds=float(raw[: -len(suffix)]) * mul)
    if raw.isdigit():
        return timedelta(seconds=int(raw))
    raise ValueError("invalid step")
