from app.models import Sample, ValueRecord, ValuesRequest, ValuesResponse, ValuesTag


def assemble(req: ValuesRequest, raw: list[Sample]) -> ValuesResponse:
    index: dict[int, int] = {}
    tags: list[ValuesTag] = []
    for tag_id in req.tags_id:
        if tag_id in index:
            continue
        index[tag_id] = len(tags)
        tags.append(ValuesTag(id=tag_id, values=[]))
    for sample in raw:
        rec = ValueRecord(date=sample.ts, value=sample.value, quality=sample.quality)
        if sample.tag_id not in index:
            index[sample.tag_id] = len(tags)
            tags.append(ValuesTag(id=sample.tag_id, values=[rec]))
            continue
        tags[index[sample.tag_id]].values.append(rec)
    return ValuesResponse(request_key=req.request_key, tags=tags)
