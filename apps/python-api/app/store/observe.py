from datetime import datetime

from app.metrics import observe_storage, track
from app.models import Sample, Tag
from app.store.base import Store


class ObservedStore:
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

    async def write(self, samples: list[Sample]) -> None:
        with track() as elapsed:
            try:
                await self._inner.write(samples)
            except Exception as exc:
                observe_storage(self.name, "write", elapsed(), exc)
                raise
        observe_storage(self.name, "write", elapsed())

    async def locf(self, tag_ids: list[int], at: datetime) -> list[Sample]:
        with track() as elapsed:
            try:
                result = await self._inner.locf(tag_ids, at)
            except Exception as exc:
                observe_storage(self.name, "locf", elapsed(), exc)
                raise
        observe_storage(self.name, "locf", elapsed())
        return result

    async def range(self, tag_ids: list[int], start: datetime, end: datetime) -> list[Sample]:
        with track() as elapsed:
            try:
                result = await self._inner.range(tag_ids, start, end)
            except Exception as exc:
                observe_storage(self.name, "range", elapsed(), exc)
                raise
        observe_storage(self.name, "range", elapsed())
        return result

    async def upsert_tags(self, tags: list[Tag]) -> None:
        with track() as elapsed:
            try:
                await self._inner.upsert_tags(tags)
            except Exception as exc:
                observe_storage(self.name, "tags", elapsed(), exc)
                raise
        observe_storage(self.name, "tags", elapsed())

    async def list_tags(self) -> list[Tag]:
        with track() as elapsed:
            try:
                result = await self._inner.list_tags()
            except Exception as exc:
                observe_storage(self.name, "tags", elapsed(), exc)
                raise
        observe_storage(self.name, "tags", elapsed())
        return result

    async def close(self) -> None:
        await self._inner.close()
