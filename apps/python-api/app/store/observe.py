from datetime import datetime, timedelta

from app.metrics import observe_storage, track
from app.models import Point, QueryResult
from app.store.base import Store


class ObservedStore:
    """Wrap any store so a new adapter inherits storage-layer metrics."""

    def __init__(self, inner: Store) -> None:
        self._inner = inner
        self.name = inner.name

    async def ping(self) -> None:
        with track() as elapsed:
            try:
                await self._inner.ping()
            except Exception as exc:
                observe_storage(self.name, "ping", elapsed(), exc)
                raise
        observe_storage(self.name, "ping", elapsed())

    async def write(self, points: list[Point]) -> None:
        with track() as elapsed:
            try:
                await self._inner.write(points)
            except Exception as exc:
                observe_storage(self.name, "write", elapsed(), exc)
                raise
        observe_storage(self.name, "write", elapsed())

    async def query(
        self,
        metric: str,
        start: datetime,
        end: datetime,
        step: timedelta,
        agg: str,
        labels: dict[str, str],
    ) -> QueryResult:
        with track() as elapsed:
            try:
                result = await self._inner.query(metric, start, end, step, agg, labels)
            except Exception as exc:
                observe_storage(self.name, "query", elapsed(), exc)
                raise
        observe_storage(self.name, "query", elapsed())
        return result

    async def latest(self, metric: str, labels: dict[str, str]) -> Point | None:
        with track() as elapsed:
            try:
                result = await self._inner.latest(metric, labels)
            except Exception as exc:
                observe_storage(self.name, "latest", elapsed(), exc)
                raise
        observe_storage(self.name, "latest", elapsed(), not_found=result is None)
        return result

    async def close(self) -> None:
        await self._inner.close()
