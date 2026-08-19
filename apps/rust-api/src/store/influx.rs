use std::fmt::Write as _;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use url::Url;

use super::{append_ilp_float, http_client, Catalog, Result, Store, StoreError};
use crate::model::{Sample, Tag};

pub struct Influx {
    base: String,
    write_url: Url,
    auth: String,
    org: String,
    bucket: String,
    client: reqwest::Client,
    tags: Catalog,
}

impl Influx {
    pub fn new(url: &str, token: &str, org: &str, bucket: &str) -> Self {
        let base = url.trim_end_matches('/').to_string();
        let mut write_url = Url::parse(&format!("{base}/api/v2/write")).expect("influx write url");
        write_url
            .query_pairs_mut()
            .append_pair("org", org)
            .append_pair("bucket", bucket)
            .append_pair("precision", "ns");
        Self {
            base,
            write_url,
            auth: format!("Token {token}"),
            org: org.to_string(),
            bucket: bucket.to_string(),
            client: http_client(),
            tags: Catalog::default(),
        }
    }

    async fn query_flux(&self, flux: &str, carried: bool) -> Result<Vec<Sample>> {
        let mut u = Url::parse(&format!("{}/api/v2/query", self.base))?;
        u.query_pairs_mut().append_pair("org", &self.org);
        let resp = self
            .client
            .post(u)
            .header("Authorization", self.auth.as_str())
            .header("Content-Type", "application/vnd.flux")
            .header("Accept", "application/csv")
            .body(flux.to_string())
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if status.as_u16() >= 300 {
            return Err(StoreError::new(format!("influx query {status}: {text}")));
        }
        parse_flux_csv(&text, carried)
    }

    async fn query_last(
        &self,
        tag_ids: &[u32],
        start: Option<DateTime<Utc>>,
        stop: DateTime<Utc>,
        carried: bool,
    ) -> Result<Vec<Sample>> {
        let start_raw = start
            .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
            .unwrap_or_else(|| "-30d".to_string());
        let flux = format!(
            r#"
from(bucket: {bucket:?})
  |> range(start: {start_raw}, stop: {stop})
  |> filter(fn: (r) => r._measurement == "samples")
  |> filter(fn: (r) => {filter})
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["tag_id"])
  |> last()
"#,
            bucket = self.bucket,
            stop = stop.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            filter = influx_tag_filter(tag_ids),
        );
        self.query_flux(&flux, carried).await
    }

    async fn query_window(&self, tag_ids: &[u32], from: DateTime<Utc>, to: DateTime<Utc>) -> Result<Vec<Sample>> {
        let start = from + chrono::Duration::nanoseconds(1);
        let stop = to + chrono::Duration::nanoseconds(1);
        let flux = format!(
            r#"
from(bucket: {bucket:?})
  |> range(start: {start}, stop: {stop})
  |> filter(fn: (r) => r._measurement == "samples")
  |> filter(fn: (r) => {filter})
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
"#,
            bucket = self.bucket,
            start = start.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            stop = stop.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            filter = influx_tag_filter(tag_ids),
        );
        self.query_flux(&flux, false).await
    }
}

fn influx_tag_filter(ids: &[u32]) -> String {
    if ids.is_empty() {
        return "true".to_string();
    }
    ids.iter()
        .map(|id| format!(r#"r.tag_id == "{id}""#))
        .collect::<Vec<_>>()
        .join(" or ")
}

fn append_ilp_line(buf: &mut String, p: &Sample) {
    let _ = write!(buf, "samples,tag_id={} value=", p.tag_id);
    append_ilp_float(buf, p.value);
    let _ = write!(
        buf,
        ",quality={}i {}\n",
        p.quality,
        p.ts.timestamp_nanos_opt().unwrap_or(0)
    );
}

fn parse_flux_csv(text: &str, carried: bool) -> Result<Vec<Sample>> {
    let mut headers: Vec<String> = Vec::new();
    let mut out = Vec::new();
    for line in text.lines() {
        if line.is_empty() {
            headers.clear();
            continue;
        }
        if line.starts_with('#') {
            continue;
        }
        let cols: Vec<&str> = line.split(',').collect();
        if headers.is_empty() {
            headers = cols.iter().map(|s| (*s).to_string()).collect();
            continue;
        }
        let get = |name: &str| -> Option<&str> {
            headers
                .iter()
                .position(|h| h == name)
                .and_then(|i| cols.get(i).copied())
                .filter(|s| !s.is_empty())
        };
        let Some(ts_raw) = get("_time") else {
            continue;
        };
        let Some(id_raw) = get("tag_id") else {
            continue;
        };
        let Some(val_raw) = get("value") else {
            continue;
        };
        let ts = DateTime::parse_from_rfc3339(ts_raw)
            .map_err(|e| StoreError::new(format!("influx ts: {e}")))?
            .with_timezone(&Utc);
        let tag_id: u32 = id_raw.parse().unwrap_or(0);
        let value: f64 = val_raw.parse().unwrap_or(0.0);
        let quality: u16 = get("quality").and_then(|s| s.parse().ok()).unwrap_or(0);
        out.push(Sample {
            ts,
            tag_id,
            value,
            quality,
            carried,
        });
    }
    Ok(out)
}

#[async_trait]
impl Store for Influx {
    fn name(&self) -> &'static str {
        "influxdb"
    }

    async fn ping(&self) -> Result<()> {
        let resp = self.client.get(format!("{}/health", self.base)).send().await?;
        if resp.status().as_u16() >= 300 {
            return Err(StoreError::new(format!("influxdb ping {}", resp.status())));
        }
        Ok(())
    }

    async fn write(&self, samples: &[Sample]) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }
        let mut body = String::with_capacity(samples.len() * 48);
        for p in samples {
            append_ilp_line(&mut body, p);
        }
        let resp = self
            .client
            .post(self.write_url.clone())
            .header("Authorization", self.auth.as_str())
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(body)
            .send()
            .await?;
        let status = resp.status();
        if status.as_u16() >= 300 {
            let text = resp.text().await.unwrap_or_default();
            return Err(StoreError::new(format!("influx write {status}: {text}")));
        }
        Ok(())
    }

    async fn locf(&self, tag_ids: &[u32], at: DateTime<Utc>) -> Result<Vec<Sample>> {
        self.query_last(tag_ids, None, at, false).await
    }

    async fn range(&self, tag_ids: &[u32], from: DateTime<Utc>, to: DateTime<Utc>) -> Result<Vec<Sample>> {
        let mut seed = self.query_last(tag_ids, None, from, true).await?;
        let mid = self.query_window(tag_ids, from, to).await?;
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
