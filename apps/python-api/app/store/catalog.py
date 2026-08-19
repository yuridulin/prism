from app.models import Tag


class CatalogMem:
    def __init__(self) -> None:
        self._data: dict[int, Tag] = {}

    def upsert(self, tags: list[Tag]) -> None:
        for tag in tags:
            self._data[tag.id] = tag

    def list(self) -> list[Tag]:
        return [self._data[k] for k in sorted(self._data)]
