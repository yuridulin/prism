import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import ORJSONResponse, PlainTextResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.exceptions import HTTPException as StarletteHTTPException
import orjson

from app.config import SUPPORTED, settings
from app.errors import http_error_handler, validation_error_handler
from app.metrics import observe_api, observe_backend, track
from app.models import (
    CONTRACT,
    OPS,
    Meta,
    TagList,
    TagWriteRequest,
    TagWriteResponse,
    ValuesRequest,
    samples_from_payload,
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


app = FastAPI(title="Prism Python API", lifespan=lifespan, default_response_class=ORJSONResponse)
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


def _meta() -> Meta:
    return Meta(backend="python", storage=store.name, storages=SUPPORTED, contract=CONTRACT, ops=OPS)


@app.get("/api/meta", response_model=Meta)
@app.get("/v1/meta", response_model=Meta)
async def meta() -> Meta:
    return _meta()


@app.get("/api/tags", response_model=TagList)
async def list_tags() -> TagList:
    return TagList(tags=await store.list_tags())


@app.post("/api/tags", response_model=TagWriteResponse)
async def upsert_tags(req: TagWriteRequest) -> TagWriteResponse:
    if not req.tags:
        raise HTTPException(status_code=400, detail="tags is required")
    await store.upsert_tags(req.tags)
    return TagWriteResponse(upserted=len(req.tags))


@app.put("/api/values")
async def write_values(request: Request) -> dict:
    try:
        payload = orjson.loads(await request.body())
        samples = samples_from_payload(payload, datetime.now(timezone.utc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except orjson.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="values array is required") from exc
    with track() as elapsed:
        try:
            await store.write(samples)
        except Exception as exc:
            observe_backend(store.name, "write", "http", 0, elapsed(), exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc
    observe_backend(store.name, "write", "http", len(samples), elapsed())
    return {"written": len(samples)}


@app.post("/api/values")
async def read_values(req: ValuesRequest) -> dict:
    if not req.tags_id:
        raise HTTPException(status_code=400, detail="tagsId is required")
    mode = req.mode()
    with track() as elapsed:
        try:
            if mode == "range":
                raw = await store.range(req.tags_id, req.old, req.young)
            else:
                raw = await store.locf(req.tags_id, req.at())
        except HTTPException:
            raise
        except Exception as exc:
            observe_backend(store.name, mode, "http", 0, elapsed(), exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc
    observe_backend(store.name, mode, "http", len(raw), elapsed())
    return assemble(req, raw)
