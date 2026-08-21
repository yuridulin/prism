from datetime import datetime, timezone

from app.models import Sample, ValuesRequest


def _fmt_date(ts: datetime) -> str:
    if ts.tzinfo is None:
        stamp = ts.replace(tzinfo=timezone.utc)
    else:
        stamp = ts.astimezone(timezone.utc)
    return stamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def assemble(req: ValuesRequest, raw: list[Sample]) -> dict:
    index: dict[int, int] = {}
    tags: list[dict] = []
    for tag_id in req.tags_id:
        if tag_id in index:
            continue
        index[tag_id] = len(tags)
        tags.append({"id": tag_id, "values": []})
    for sample in raw:
        rec = {"date": _fmt_date(sample.ts), "value": sample.value, "quality": sample.quality}
        if sample.tag_id not in index:
            index[sample.tag_id] = len(tags)
            tags.append({"id": sample.tag_id, "values": [rec]})
            continue
        tags[index[sample.tag_id]]["values"].append(rec)
    return {"requestKey": req.request_key, "tags": tags}
