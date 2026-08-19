from datetime import datetime, timedelta

from app.models import ReadRequest, ReadResult, Sample, Series


def group_by_tag(tag_ids: list[int], samples: list[Sample]) -> list[Series]:
    index: dict[int, int] = {}
    out: list[Series] = []
    for tag_id in tag_ids:
        if tag_id in index:
            continue
        index[tag_id] = len(out)
        out.append(Series(tag_id=tag_id, samples=[]))
    for sample in samples:
        if sample.tag_id not in index:
            index[sample.tag_id] = len(out)
            out.append(Series(tag_id=sample.tag_id, samples=[sample]))
            continue
        out[index[sample.tag_id]].samples.append(sample)
    return out


def _last_at_or_before(samples: list[Sample], at: datetime) -> Sample | None:
    last: Sample | None = None
    for sample in samples:
        if sample.ts > at:
            continue
        if last is None or sample.ts > last.ts:
            last = sample
    return last


def resample(series: list[Series], start: datetime, end: datetime, step: timedelta) -> list[Series]:
    if step.total_seconds() <= 0:
        step = timedelta(minutes=1)
    out: list[Series] = []
    for src in series:
        dst = Series(tag_id=src.tag_id, samples=[])
        tick = start
        while tick < end or tick == start:
            if tick >= end and tick != start:
                break
            last = _last_at_or_before(src.samples, tick)
            if last is not None:
                dst.samples.append(
                    Sample(
                        ts=tick,
                        tag_id=src.tag_id,
                        value=last.value,
                        quality=last.quality,
                        carried=last.carried or last.ts != tick,
                    )
                )
            tick += step
        out.append(dst)
    return out


def time_weighted_avg(series: list[Series], start: datetime, end: datetime) -> list[Series]:
    out: list[Series] = []
    for src in series:
        dst = Series(tag_id=src.tag_id, samples=[])
        weighted = 0.0
        weight = 0.0
        points = list(src.samples)
        for i, sample in enumerate(points):
            left = start if sample.ts < start else sample.ts
            right = end
            if i + 1 < len(points) and points[i + 1].ts < end:
                right = points[i + 1].ts
            if right <= left:
                continue
            dt = (right - left).total_seconds()
            weighted += sample.value * dt
            weight += dt
        if weight > 0:
            dst.value = weighted / weight
        out.append(dst)
    return out


def assemble(req: ReadRequest, raw: list[Sample], step: timedelta | None = None) -> ReadResult:
    series = group_by_tag(req.tag_ids, raw)
    result = ReadResult(mode=req.mode, series=series, at=req.at, from_=req.from_, to=req.to)
    if req.mode == "sample":
        assert req.from_ is not None and req.to is not None
        result.step = req.step or "1m"
        result.series = resample(series, req.from_, req.to, step or timedelta(minutes=1))
    elif req.mode == "twavg":
        assert req.from_ is not None and req.to is not None
        result.series = time_weighted_avg(series, req.from_, req.to)
    return result
