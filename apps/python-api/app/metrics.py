"""Three-layer telemetry: api / backend / storage.

Add an op by calling observe_backend / observe_storage with the same names.
Native DB exporters are scraped separately by Prometheus.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from prometheus_client import Counter, Gauge, Histogram

BACKEND = "python"

API_REQUESTS = Counter(
    "prism_api_requests_total",
    "HTTP requests by route",
    ["backend", "storage", "route", "method", "status"],
)
API_DURATION = Histogram(
    "prism_api_request_duration_seconds",
    "HTTP request latency",
    ["backend", "storage", "route", "method"],
)
BACKEND_OPS = Counter(
    "prism_backend_ops_total",
    "Application operations",
    ["backend", "storage", "op", "source", "result"],
)
BACKEND_DURATION = Histogram(
    "prism_backend_op_duration_seconds",
    "Application operation latency",
    ["backend", "storage", "op", "source"],
)
BACKEND_ITEMS = Counter(
    "prism_backend_items_total",
    "Points written or samples returned",
    ["backend", "storage", "op", "source"],
)
STORAGE_OPS = Counter(
    "prism_storage_ops_total",
    "Storage adapter operations",
    ["backend", "storage", "op", "result"],
)
STORAGE_DURATION = Histogram(
    "prism_storage_op_duration_seconds",
    "Storage adapter latency",
    ["backend", "storage", "op"],
)
STORAGE_UP = Gauge(
    "prism_storage_up",
    "1 if the last storage ping succeeded",
    ["backend", "storage"],
)


def _result(exc: BaseException | None, not_found: bool = False) -> str:
    if not_found:
        return "not_found"
    if exc is None:
        return "ok"
    return "error"


def observe_api(storage: str, route: str, method: str, status: str, seconds: float) -> None:
    API_REQUESTS.labels(BACKEND, storage, route, method, status).inc()
    API_DURATION.labels(BACKEND, storage, route, method).observe(seconds)


def observe_backend(
    storage: str,
    op: str,
    source: str,
    items: int,
    seconds: float,
    exc: BaseException | None = None,
    not_found: bool = False,
) -> None:
    result = _result(exc, not_found)
    BACKEND_OPS.labels(BACKEND, storage, op, source, result).inc()
    BACKEND_DURATION.labels(BACKEND, storage, op, source).observe(seconds)
    if exc is None and items > 0:
        BACKEND_ITEMS.labels(BACKEND, storage, op, source).inc(items)


def observe_storage(
    storage: str,
    op: str,
    seconds: float,
    exc: BaseException | None = None,
    not_found: bool = False,
) -> None:
    STORAGE_OPS.labels(BACKEND, storage, op, _result(exc, not_found)).inc()
    STORAGE_DURATION.labels(BACKEND, storage, op).observe(seconds)
    if op == "ping":
        STORAGE_UP.labels(BACKEND, storage).set(0 if exc else 1)


@contextmanager
def track() -> Iterator[Callable[[], float]]:
    start = time.perf_counter()
    yield lambda: time.perf_counter() - start
