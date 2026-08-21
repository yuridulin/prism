use crate::model::{Sample, ValueRecord, ValuesRequest, ValuesResponse, ValuesTag};

pub fn assemble(req: &ValuesRequest, raw: &[Sample]) -> ValuesResponse {
    let mut out: Vec<ValuesTag> = Vec::with_capacity(req.tags_id.len());
    let mut index = std::collections::HashMap::with_capacity(req.tags_id.len());
    for &id in &req.tags_id {
        if index.contains_key(&id) {
            continue;
        }
        index.insert(id, out.len());
        out.push(ValuesTag {
            id,
            values: Vec::new(),
        });
    }
    for s in raw {
        let rec = ValueRecord {
            date: s.ts,
            value: s.value,
            quality: s.quality,
        };
        if let Some(&i) = index.get(&s.tag_id) {
            out[i].values.push(rec);
        } else {
            index.insert(s.tag_id, out.len());
            out.push(ValuesTag {
                id: s.tag_id,
                values: vec![rec],
            });
        }
    }
    ValuesResponse {
        request_key: req.request_key.clone(),
        tags: out,
    }
}
