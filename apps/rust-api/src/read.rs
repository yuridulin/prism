use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::model::{ReadRequest, ReadResult, Sample, Series};

pub fn parse_step(raw: &str) -> Duration {
    if raw.is_empty() {
        return Duration::from_secs(60);
    }
    match humantime::parse_duration(raw) {
        Ok(d) if d > Duration::ZERO => d,
        _ => Duration::from_secs(60),
    }
}

/// Keep input order of `tag_ids` and attach samples (already ordered by ts per tag).
pub fn group_by_tag(tag_ids: &[u32], samples: &[Sample]) -> Vec<Series> {
    let mut out: Vec<Series> = Vec::with_capacity(tag_ids.len());
    let mut index = std::collections::HashMap::with_capacity(tag_ids.len());
    for &id in tag_ids {
        if index.contains_key(&id) {
            continue;
        }
        index.insert(id, out.len());
        out.push(Series {
            tag_id: id,
            value: None,
            samples: Vec::new(),
        });
    }
    for s in samples {
        if let Some(&i) = index.get(&s.tag_id) {
            out[i].samples.push(s.clone());
        } else {
            index.insert(s.tag_id, out.len());
            out.push(Series {
                tag_id: s.tag_id,
                value: None,
                samples: vec![s.clone()],
            });
        }
    }
    out
}

fn last_at_or_before(samples: &[Sample], at: DateTime<Utc>) -> Option<&Sample> {
    let mut last: Option<&Sample> = None;
    for s in samples {
        if s.ts > at {
            continue;
        }
        if last.map(|l| s.ts > l.ts).unwrap_or(true) {
            last = Some(s);
        }
    }
    last
}

/// Stretch the last observation onto a regular grid `[from, to)`.
pub fn resample(series: &[Series], from: DateTime<Utc>, to: DateTime<Utc>, step: Duration) -> Vec<Series> {
    let step = if step.is_zero() {
        Duration::from_secs(60)
    } else {
        step
    };
    let delta = chrono::Duration::from_std(step).unwrap_or(chrono::Duration::minutes(1));
    series
        .iter()
        .map(|src| {
            let mut dst = Series {
                tag_id: src.tag_id,
                value: None,
                samples: Vec::new(),
            };
            let mut t = from;
            loop {
                if t > to || (t == to && t != from) {
                    break;
                }
                if let Some(last) = last_at_or_before(&src.samples, t) {
                    dst.samples.push(Sample {
                        ts: t,
                        tag_id: src.tag_id,
                        value: last.value,
                        quality: last.quality,
                        carried: last.carried || last.ts != t,
                    });
                }
                let next = t + delta;
                if next <= t {
                    break;
                }
                t = next;
            }
            dst
        })
        .collect()
}

/// Weight each value by how long it was current in `[from, to]`.
pub fn time_weighted_avg(series: &[Series], from: DateTime<Utc>, to: DateTime<Utc>) -> Vec<Series> {
    let span = to - from;
    if span <= chrono::Duration::zero() {
        return Vec::new();
    }
    series
        .iter()
        .map(|src| {
            let mut dst = Series {
                tag_id: src.tag_id,
                value: None,
                samples: Vec::new(),
            };
            let mut weighted = 0.0;
            let mut weight = 0.0;
            for (j, s) in src.samples.iter().enumerate() {
                let mut start = s.ts;
                if start < from {
                    start = from;
                }
                let mut end = to;
                if j + 1 < src.samples.len() && src.samples[j + 1].ts < to {
                    end = src.samples[j + 1].ts;
                }
                if end <= start {
                    continue;
                }
                let dt = (end - start).num_nanoseconds().unwrap_or(0) as f64 / 1_000_000_000.0;
                weighted += s.value * dt;
                weight += dt;
            }
            if weight > 0.0 {
                dst.value = Some(weighted / weight);
            }
            dst
        })
        .collect()
}

pub fn assemble(mode: &str, req: &ReadRequest, raw: &[Sample]) -> ReadResult {
    let series = group_by_tag(&req.tag_ids, raw);
    match mode {
        "locf" => ReadResult {
            mode: mode.to_string(),
            at: req.at,
            from: None,
            to: None,
            step: String::new(),
            series,
        },
        "range" => ReadResult {
            mode: mode.to_string(),
            at: None,
            from: req.from,
            to: req.to,
            step: String::new(),
            series,
        },
        "sample" => {
            let step_raw = if req.step.is_empty() {
                "1m"
            } else {
                req.step.as_str()
            };
            let from = req.from.unwrap_or(Utc::now());
            let to = req.to.unwrap_or(from);
            ReadResult {
                mode: mode.to_string(),
                at: None,
                from: req.from,
                to: req.to,
                step: step_raw.to_string(),
                series: resample(&series, from, to, parse_step(step_raw)),
            }
        }
        "twavg" => {
            let from = req.from.unwrap_or(Utc::now());
            let to = req.to.unwrap_or(from);
            ReadResult {
                mode: mode.to_string(),
                at: None,
                from: req.from,
                to: req.to,
                step: String::new(),
                series: time_weighted_avg(&series, from, to),
            }
        }
        _ => ReadResult {
            mode: mode.to_string(),
            at: None,
            from: None,
            to: None,
            step: String::new(),
            series,
        },
    }
}
