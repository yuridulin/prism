use std::collections::HashMap;
use std::fmt::Write as _;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use url::Url;

use super::{append_ilp_float, Catalog, Result, Store, StoreError};
use crate::model::{Sample, Tag};

pub struct VictoriaMetrics {
    base: String,
    write_url: String,
    client: reqwest::Client,
    tags: Catalog,
}

impl VictoriaMetrics {
    pub fn new(base: &str) -> Self {
        let base = base.trim_end_matches('/').to_string();
        Self {
            write_url: format!("{base}/write?precision=ms"),
            base,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .pool_max_idle_per_host(16)
                .build()
                .expect("vm http client"),
            tags: Catalog::default(),
        }
    }

    fn tag_re(ids: &[u32]) -> String {
        let mut out = String::new();
        for (i, id) in ids.iter().enumerate() {
            if i > 0 {
                out.push('|');
            }
            let _ = write!(out, "{id}");
        }
        out
    }

    async fn scan(
        &self,
        tag_ids: &[u32],
        export_start: DateTime<Utc>,
        export_end: DateTime<Utc>,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        with_mid: bool,
    ) -> Result<(Vec<Sample>, Vec<Sample>)> {
        if tag_ids.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let mut u = Url::parse(&format!("{}/api/v1/export/csv", self.base))?;
        u.query_pairs_mut()
            .append_pair("match[]", &format!(r#"prism_sample{{tag_id=~"{}"}}"#, Self::tag_re(tag_ids)))
            .append_pair("start", &export_start.timestamp().to_string())
            .append_pair("end", &export_end.timestamp().to_string())
            .append_pair("format", "tag_id,quality,__value__,__timestamp__:unix_ms");
        let resp = self.client.get(u).send().await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if status.as_u16() >= 300 {
            return Err(StoreError::new(format!("vm export {status}: {text}")));
        }

        let mut best: HashMap<u32, Sample> = HashMap::new();
        let mut mid = Vec::new();
        for line in text.lines() {
            if line.trim().is_empty() || line.starts_with("tag_id") {
                continue;
            }
            let mut parts = line.split(',');
            let Some(id_raw) = parts.next() else {
                continue;
            };
            let Some(q_raw) = parts.next() else {
                continue;
            };
            let Some(val_raw) = parts.next() else {
                continue;
            };
            let Some(ts_raw) = parts.next() else {
                continue;
            };
            let Ok(id) = id_raw.parse::<u32>() else {
                continue;
            };
            let q: u16 = q_raw.parse().unwrap_or(0);
            let val: f64 = val_raw.parse().unwrap_or(0.0);
            let ms: i64 = ts_raw.parse().unwrap_or(0);
            let t = DateTime::from_timestamp_millis(ms)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            if t <= from {
                let better = best.get(&id).map(|prev| t > prev.ts).unwrap_or(true);
                if better {
                    best.insert(
                        id,
                        Sample {
                            ts: t,
                            tag_id: id,
                            value: val,
                            quality: q,
                            carried: with_mid,
                        },
                    );
                }
                continue;
            }
            if with_mid && t <= to {
                mid.push(Sample {
                    ts: t,
                    tag_id: id,
                    value: val,
                    quality: q,
                    carried: false,
                });
            }
        }
        let seed = tag_ids
            .iter()
            .filter_map(|id| best.remove(id))
            .collect();
        Ok((seed, mid))
    }
}

#[async_trait]
impl Store for VictoriaMetrics {
    fn name(&self) -> &'static str {
        "victoriametrics"
    }

    async fn ping(&self) -> Result<()> {
        let resp = self.client.get(format!("{}/health", self.base)).send().await?;
        if resp.status().as_u16() >= 300 {
            return Err(StoreError::new(format!("vm health status {}", resp.status())));
        }
        Ok(())
    }

    async fn write(&self, samples: &[Sample]) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }
        let mut buf = String::with_capacity(samples.len() * 48);
        for p in samples {
            let _ = write!(buf, "prism,tag_id={},quality={} sample=", p.tag_id, p.quality);
            append_ilp_float(&mut buf, p.value);
            let _ = write!(buf, " {}\n", p.ts.timestamp_millis());
        }
        let resp = self.client.post(&self.write_url).body(buf).send().await?;
        let status = resp.status();
        if status.as_u16() >= 300 {
            let text = resp.text().await.unwrap_or_default();
            return Err(StoreError::new(format!("vm write status {status}: {text}")));
        }
        Ok(())
    }

    async fn locf(&self, tag_ids: &[u32], at: DateTime<Utc>) -> Result<Vec<Sample>> {
        // Archive max gap is 1h; 2h lookback is enough even at 364d ago.
        let (seed, _) = self
            .scan(tag_ids, at - Duration::hours(2), at, at, at, false)
            .await?;
        Ok(seed)
    }

    async fn range(&self, tag_ids: &[u32], from: DateTime<Utc>, to: DateTime<Utc>) -> Result<Vec<Sample>> {
        let (mut seed, mid) = self
            .scan(tag_ids, from - Duration::hours(2), to, from, to, true)
            .await?;
        seed.extend(mid);
        Ok(seed)
    }

    async fn upsert_tags(&self, tags: &[Tag]) -> Result<()> {
        self.tags.upsert(tags);
        Ok(())
    }

    async fn list_tags(&self) -> Result<Vec<Tag>> {
        Ok(self.tags.list())
    }
}

