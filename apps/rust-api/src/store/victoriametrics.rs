use std::fmt::Write as _;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use url::Url;

use super::{append_ilp_float, Catalog, Result, Store, StoreError};
use crate::model::{Sample, Tag};

pub struct VictoriaMetrics {
    base: String,
    write_url: String,
    client: reqwest::Client,
    tags: Catalog,
}

#[derive(Debug, Deserialize)]
struct VmInstantResponse {
    data: VmData,
}

#[derive(Debug, Deserialize)]
struct VmData {
    result: Vec<VmInstant>,
}

#[derive(Debug, Deserialize)]
struct VmInstant {
    metric: std::collections::HashMap<String, String>,
    value: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct VmExportRow {
    metric: std::collections::HashMap<String, String>,
    timestamps: Vec<i64>,
    values: Vec<f64>,
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

    async fn get_json<T: serde::de::DeserializeOwned>(&self, path_and_query: &str) -> Result<T> {
        let resp = self
            .client
            .get(format!("{}{path_and_query}", self.base))
            .send()
            .await?;
        let status = resp.status();
        if status.as_u16() >= 300 {
            let text = resp.text().await.unwrap_or_default();
            return Err(StoreError::new(format!("vm query {status}: {text}")));
        }
        Ok(resp.json().await?)
    }
}

fn quality_from(metric: &std::collections::HashMap<String, String>) -> u16 {
    metric
        .get("quality")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
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
        let mut out = Vec::new();
        for id in tag_ids {
            let mut params = Url::parse("http://vm.local/api/v1/query")?;
            params.query_pairs_mut()
                .append_pair("query", &format!(r#"last_over_time(prism_sample{{tag_id="{id}"}}[30d])"#))
                .append_pair("time", &at.timestamp().to_string());
            let parsed: VmInstantResponse = self
                .get_json(&format!("/api/v1/query?{}", params.query().unwrap_or("")))
                .await?;
            let Some(sample) = parsed.data.result.first() else {
                continue;
            };
            let ts = sample
                .value
                .first()
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0) as i64;
            let val = sample
                .value
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            out.push(Sample {
                ts: DateTime::from_timestamp(ts, 0).unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap()),
                tag_id: *id,
                value: val,
                quality: quality_from(&sample.metric),
                carried: false,
            });
        }
        Ok(out)
    }

    async fn range(&self, tag_ids: &[u32], from: DateTime<Utc>, to: DateTime<Utc>) -> Result<Vec<Sample>> {
        let mut seed = self.locf(tag_ids, from).await?;
        for s in &mut seed {
            s.carried = true;
        }
        let mut mid = Vec::new();
        for id in tag_ids {
            let mut u = Url::parse(&format!("{}/api/v1/export", self.base))?;
            u.query_pairs_mut()
                .append_pair("match[]", &format!(r#"prism_sample{{tag_id="{id}"}}"#))
                .append_pair("start", &from.timestamp().to_string())
                .append_pair("end", &to.timestamp().to_string());
            let resp = self.client.get(u).send().await?;
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            if status.as_u16() >= 300 {
                return Err(StoreError::new(format!("vm export {status}: {text}")));
            }
            for line in text.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                let row: VmExportRow = serde_json::from_str(line)
                    .map_err(|e| StoreError::new(format!("vm export json: {e}")))?;
                let q = quality_from(&row.metric);
                for (i, ts) in row.timestamps.iter().enumerate() {
                    let t = DateTime::from_timestamp_millis(*ts)
                        .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
                    if t <= from || t > to {
                        continue;
                    }
                    let val = row.values.get(i).copied().unwrap_or(0.0);
                    mid.push(Sample {
                        ts: t,
                        tag_id: *id,
                        value: val,
                        quality: q,
                        carried: false,
                    });
                }
            }
        }
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

