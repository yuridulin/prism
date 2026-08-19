import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import PlainTextResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.config import SUPPORTED, settings
from app.errors import http_error_handler, validation_error_handler
from app.metrics import observe_api, observe_backend, track
from app.models import (
    CONTRACT,
    OPS,
    Meta,
    ReadRequest,
    ReadResult,
    TagList,
    TagWriteRequest,
    TagWriteResponse,
    WriteRequest,
    WriteResponse,
)
from app.nats_consumer import run_consumer
from app.read import assemble
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


@app.get("/v1/tags", response_model=TagList)
async def list_tags() -> TagList:
    return TagList(tags=await store.list_tags())


@app.post("/v1/tags", response_model=TagWriteResponse)
async def upsert_tags(req: TagWriteRequest) -> TagWriteResponse:
    if not req.tags:
        raise HTTPException(status_code=400, detail="tags is required")
    await store.upsert_tags(req.tags)
    return TagWriteResponse(upserted=len(req.tags))


@app.post("/v1/write", response_model=WriteResponse)
async def write_samples(req: WriteRequest) -> WriteResponse:
    if not req.samples:
        raise HTTPException(status_code=400, detail="samples is required")
    now = datetime.now(timezone.utc)
    for s in req.samples:
        if s.ts.tzinfo is None:
            s.ts = s.ts.replace(tzinfo=timezone.utc)
        if s.ts.timestamp() == 0:
            s.ts = now
    with track() as elapsed:
        try:
            await store.write(req.samples)
        except Exception as exc:
            observe_backend(store.name, "write", "http", 0, elapsed(), exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc
    observe_backend(store.name, "write", "http", len(req.samples), elapsed())
    return WriteResponse(written=len(req.samples))


@app.post("/v1/read", response_model=ReadResult)
async def read_post(req: ReadRequest) -> ReadResult:
    return await _read(req)


@app.post("/v1/locf", response_model=ReadResult)
async def locf_post(req: ReadRequest) -> ReadResult:
    req.mode = "locf"
    return await _read(req)


@app.post("/v1/range", response_model=ReadResult)
async def range_post(req: ReadRequest) -> ReadResult:
    req.mode = "range"
    return await _read(req)


async def _read(req: ReadRequest) -> ReadResult:
    if not req.tag_ids:
        raise HTTPException(status_code=400, detail="tag_ids is required")
    step = _parse_step(req.step)
    with track() as elapsed:
        try:
            if req.mode == "locf":
                if req.at is None:
                    raise HTTPException(status_code=400, detail="at is required")
                raw = await store.locf(req.tag_ids, req.at)
            else:
                if req.from_ is None or req.to is None:
                    raise HTTPException(status_code=400, detail="from and to are required")
                raw = await store.range(req.tag_ids, req.from_, req.to)
        except HTTPException:
            raise
        except Exception as exc:
            observe_backend(store.name, req.mode, "http", 0, elapsed(), exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc
    observe_backend(store.name, req.mode, "http", len(raw), elapsed())
    return assemble(req, raw, step)


def _parse_step(raw: str) -> timedelta:
    units = {"ms": 0.001, "s": 1, "m": 60, "h": 3600}
    for suffix, mul in units.items():
        if raw.endswith(suffix):
            return timedelta(seconds=float(raw[: -len(suffix)]) * mul)
    if raw.isdigit():
        return timedelta(seconds=int(raw))
    raise HTTPException(status_code=400, detail="invalid step")
