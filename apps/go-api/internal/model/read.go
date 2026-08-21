package model

// GroupByTag keeps input order of tagIDs and sorts samples by ts.
func GroupByTag(tagIDs []uint32, samples []Sample) []ValuesTag {
	index := make(map[uint32]int, len(tagIDs))
	out := make([]ValuesTag, 0, len(tagIDs))
	for _, id := range tagIDs {
		if _, ok := index[id]; ok {
			continue
		}
		index[id] = len(out)
		out = append(out, ValuesTag{ID: id, Values: []ValueRecord{}})
	}
	for _, s := range samples {
		rec := ValueRecord{Date: s.TS, Value: s.Value, Quality: s.Quality}
		i, ok := index[s.TagID]
		if !ok {
			index[s.TagID] = len(out)
			out = append(out, ValuesTag{ID: s.TagID, Values: []ValueRecord{rec}})
			continue
		}
		out[i].Values = append(out[i].Values, rec)
	}
	return out
}

func Assemble(req ValuesRequest, raw []Sample) ValuesResponse {
	tags := GroupByTag(req.TagsID, raw)
	return ValuesResponse{RequestKey: req.RequestKey, Tags: tags}
}
