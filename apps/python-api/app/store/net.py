from datetime import datetime, timezone
from urllib.parse import unquote, urlparse

import httpx

LIMITS = httpx.Limits(max_connections=64, max_keepalive_connections=32)


def new_client(timeout: float = 30.0, **kwargs) -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=timeout, limits=LIMITS, trust_env=False, **kwargs)


def ilp_float(value: float) -> str:
    text = repr(float(value))
    if text in {"nan", "inf", "-inf"}:
        return "0.0"
    return text


def unix_ns(ts: datetime) -> int:
    return int(_utc(ts).timestamp() * 1_000_000_000)


def unix_ms(ts: datetime) -> int:
    return int(_utc(ts).timestamp() * 1000)


def _utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def parse_hostport(addr: str, default_port: int) -> tuple[str, int]:
    raw = addr.strip()
    for prefix in ("tcp://", "http://", "https://"):
        if raw.startswith(prefix):
            raw = raw[len(prefix) :]
    host, sep, port = raw.rpartition(":")
    if not sep:
        return raw or "localhost", default_port
    return host or "localhost", int(port)


def split_http_url(url: str) -> tuple[str, tuple[str, str] | None]:
    parsed = urlparse(url)
    user = unquote(parsed.username) if parsed.username else None
    password = unquote(parsed.password) if parsed.password else ""
    host = parsed.hostname or "localhost"
    port = parsed.port
    origin = f"{parsed.scheme}://{host}"
    if port:
        origin += f":{port}"
    auth = (user, password) if user is not None else None
    return origin.rstrip("/") + "/", auth


def raise_status(resp: httpx.Response, what: str) -> None:
    if resp.status_code >= 300:
        raise RuntimeError(f"{what} {resp.status_code}: {resp.text[:500]}")
