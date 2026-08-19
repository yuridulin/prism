package model

import "time"

// GroupByTag keeps input order of tagIDs and sorts samples by ts.
func GroupByTag(tagIDs []uint32, samples []Sample) []Series {
	index := make(map[uint32]int, len(tagIDs))
	out := make([]Series, 0, len(tagIDs))
	for _, id := range tagIDs {
		if _, ok := index[id]; ok {
			continue
		}
		index[id] = len(out)
		out = append(out, Series{TagID: id, Samples: []Sample{}})
	}
	for _, s := range samples {
		i, ok := index[s.TagID]
		if !ok {
			index[s.TagID] = len(out)
			out = append(out, Series{TagID: s.TagID, Samples: []Sample{s}})
			continue
		}
		out[i].Samples = append(out[i].Samples, s)
	}
	return out
}

func lastAtOrBefore(samples []Sample, at time.Time) (Sample, bool) {
	var last Sample
	found := false
	for _, s := range samples {
		if s.TS.After(at) {
			continue
		}
		if !found || s.TS.After(last.TS) {
			last = s
			found = true
		}
	}
	return last, found
}

// Resample stretches the last observation onto a regular grid [from, to).
func Resample(series []Series, from, to time.Time, step time.Duration) []Series {
	if step <= 0 {
		step = time.Minute
	}
	out := make([]Series, len(series))
	for i, src := range series {
		dst := Series{TagID: src.TagID, Samples: []Sample{}}
		for t := from; t.Before(to) || t.Equal(from); t = t.Add(step) {
			if t.After(to) || t.Equal(to) && !t.Equal(from) {
				break
			}
			last, ok := lastAtOrBefore(src.Samples, t)
			if !ok {
				continue
			}
			carried := last.Carried || !last.TS.Equal(t)
			dst.Samples = append(dst.Samples, Sample{
				TS:      t.UTC(),
				TagID:   src.TagID,
				Value:   last.Value,
				Quality: last.Quality,
				Carried: carried,
			})
		}
		out[i] = dst
	}
	return out
}

// TimeWeightedAvg weights each value by how long it was current in [from, to].
func TimeWeightedAvg(series []Series, from, to time.Time) []Series {
	out := make([]Series, len(series))
	span := to.Sub(from)
	if span <= 0 {
		return GroupByTag(nil, nil)
	}
	for i, src := range series {
		dst := Series{TagID: src.TagID, Samples: []Sample{}}
		var weighted, weight float64
		points := append([]Sample(nil), src.Samples...)
		for j, s := range points {
			start := s.TS
			if start.Before(from) {
				start = from
			}
			end := to
			if j+1 < len(points) && points[j+1].TS.Before(to) {
				end = points[j+1].TS
			}
			if !end.After(start) {
				continue
			}
			dt := end.Sub(start).Seconds()
			weighted += s.Value * dt
			weight += dt
		}
		if weight > 0 {
			avg := weighted / weight
			dst.Value = &avg
		}
		out[i] = dst
	}
	return out
}

func Assemble(mode string, req ReadRequest, raw []Sample) ReadResult {
	series := GroupByTag(req.TagIDs, raw)
	res := ReadResult{Mode: mode, Series: series}
	switch mode {
	case "locf":
		res.At = TimePtr(req.At)
	case "range":
		res.From = TimePtr(req.From)
		res.To = TimePtr(req.To)
	case "sample":
		stepRaw := req.Step
		if stepRaw == "" {
			stepRaw = "1m"
		}
		res.From = TimePtr(req.From)
		res.To = TimePtr(req.To)
		res.Step = stepRaw
		res.Series = Resample(series, req.From.UTC(), req.To.UTC(), ParseStep(stepRaw))
	case "twavg":
		res.From = TimePtr(req.From)
		res.To = TimePtr(req.To)
		res.Series = TimeWeightedAvg(series, req.From.UTC(), req.To.UTC())
	}
	return res
}
