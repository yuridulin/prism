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

    async fn query_last(
        &self,
        tag_ids: &[u32],
        stop: DateTime<Utc>,
        carried: bool,
    ) -> Result<Vec<Sample>> {
        let q = format!(
            r#"SELECT last("value") AS "value", last("quality") AS "quality" FROM "samples" WHERE time <= {stop} AND {filter} GROUP BY "tag_id""#,
            stop = influxql_time(stop),
            filter = influxql_tag_re(tag_ids),
        );
        self.query_influxql(&q, carried).await
    }

    async fn query_window(&self, tag_ids: &[u32], from: DateTime<Utc>, to: DateTime<Utc>) -> Result<Vec<Sample>> {
        let q = format!(
            r#"SELECT "value", "quality" FROM "samples" WHERE time > {start} AND time <= {stop} AND {filter}"#,
            start = influxql_time(from),
            stop = influxql_time(to),
            filter = influxql_tag_re(tag_ids),
        );
        self.query_influxql(&q, false).await
    }

    async fn query_influxql(&self, q: &str, carried: bool) -> Result<Vec<Sample>> {
        let resp = self
            .client
            .post(format!("{}/query", self.base))
            .header("Authorization", self.auth.as_str())
            .header("Accept", "application/csv")
            .form(&[
                ("org", self.org.as_str()),
                ("bucket", self.bucket.as_str()),
                ("db", self.bucket.as_str()),
                ("epoch", "ms"),
                ("q", q),
            ])
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if status.as_u16() >= 300 {
            return Err(StoreError::new(format!("influx query {status}: {text}")));
        }
        parse_influx_body(&text, carried)
    }

    async fn ensure_dbrp(&self) -> Result<()> {
        let listed = self
            .client
            .get(format!("{}/api/v2/dbrps", self.base))
            .header("Authorization", self.auth.as_str())
            .query(&[("org", self.org.as_str()), ("db", self.bucket.as_str())])
            .send()
            .await?;
        if listed.status().as_u16() < 300 {
            let v: serde_json::Value = listed.json().await.unwrap_or(serde_json::Value::Null);
            if v.get("content").and_then(|c| c.as_array()).map(|a| !a.is_empty()).unwrap_or(false) {
                return Ok(());
            }
        }
        let buckets = self
            .client
            .get(format!("{}/api/v2/buckets", self.base))
            .header("Authorization", self.auth.as_str())
            .query(&[("org", self.org.as_str()), ("name", self.bucket.as_str())])
            .send()
            .await?;
        let status = buckets.status();
        let text = buckets.text().await.unwrap_or_default();
        if status.as_u16() >= 300 {
            return Err(StoreError::new(format!("influx buckets {status}: {text}")));
        }
        let v: serde_json::Value = serde_json::from_str(&text).map_err(StoreError::new)?;
        let id = v
            .pointer("/buckets/0/id")
            .and_then(|x| x.as_str())
            .ok_or_else(|| StoreError::new("influx bucket not found"))?;
        let body = serde_json::json!({
            "org": self.org,
            "bucketID": id,
            "database": self.bucket,
            "retention_policy": "autogen",
            "default": true
        });
        let created = self
            .client
            .post(format!("{}/api/v2/dbrps", self.base))
            .header("Authorization", self.auth.as_str())
            .query(&[("org", self.org.as_str())])
            .json(&body)
            .send()
            .await?;
        let status = created.status();
        if status.as_u16() >= 300 && status.as_u16() != 409 {
            let text = created.text().await.unwrap_or_default();
            return Err(StoreError::new(format!("influx dbrp {status}: {text}")));
        }
        Ok(())
    }
}

fn influxql_time(t: DateTime<Utc>) -> String {
    format!("'{}'", t.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
}

fn influxql_tag_re(ids: &[u32]) -> String {
    if ids.is_empty() {
        return "true".to_string();
    }
    let parts: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
    format!("tag_id =~ /^({})$/", parts.join("|"))
}

fn parse_influx_body(text: &str, carried: bool) -> Result<Vec<Sample>> {
    let trimmed = text.trim_start();
    if trimmed.is_empty() || !trimmed.starts_with('{') {
        return Ok(parse_influx_csv(text, carried));
    }
    parse_influxql(text, carried)
}

fn parse_influx_csv(text: &str, carried: bool) -> Vec<Sample> {
    let mut lines = text.lines();
    let mut idx: Option<Vec<String>> = None;
    let mut out = Vec::new();
    for line in lines.by_ref() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let cols: Vec<&str> = line.split(',').collect();
        if idx.is_none() {
            let names: Vec<String> = cols.iter().map(|c| c.trim().trim_matches('"').to_ascii_lowercase()).collect();
            if names.iter().any(|n| n == "time") {
                idx = Some(names);
            }
            continue;
        }
        let names = idx.as_ref().unwrap();
        let pos = |name: &str| names.iter().position(|n| n == name);
        let (Some(ti), Some(vi)) = (pos("time"), pos("value")) else {
            continue;
        };
        if ti >= cols.len() || vi >= cols.len() {
            continue;
        }
        let tag_id = if let Some(i) = pos("tag_id") {
            cols.get(i).and_then(|s| s.trim_matches('"').parse().ok()).unwrap_or(0)
        } else if let Some(i) = pos("tags") {
            tag_id_from_influx_tags(cols.get(i).copied().unwrap_or(""))
        } else {
            0
        };
        let quality = pos("quality")
            .and_then(|i| cols.get(i))
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0) as u16;
        let ts_raw = cols[ti].trim_matches('"');
        let ts = if let Ok(ms) = ts_raw.parse::<i64>() {
            DateTime::from_timestamp_millis(ms).unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap())
        } else {
            DateTime::parse_from_rfc3339(ts_raw)
                .map(|t| t.with_timezone(&Utc))
                .unwrap_or_else(|_| DateTime::from_timestamp(0, 0).unwrap())
        };
        let value = cols[vi].parse().unwrap_or(0.0);
        out.push(Sample {
            ts,
            tag_id,
            value,
            quality,
            carried,
        });
    }
    out
}

fn tag_id_from_influx_tags(s: &str) -> u32 {
    let s = s.trim_matches('"');
    for part in s.split(',') {
        if let Some((k, v)) = part.split_once('=') {
            if k.trim() == "tag_id" {
                return v.trim().parse().unwrap_or(0);
            }
        }
    }
    0
}

fn parse_influxql(text: &str, carried: bool) -> Result<Vec<Sample>> {
    let v: serde_json::Value = serde_json::from_str(text).map_err(StoreError::new)?;
    let mut out = Vec::new();
    let Some(results) = v.get("results").and_then(|r| r.as_array()) else {
        return Ok(out);
    };
    for result in results {
        if let Some(err) = result.get("error").and_then(|e| e.as_str()) {
            return Err(StoreError::new(format!("influxql: {err}")));
        }
        let Some(series) = result.get("series").and_then(|s| s.as_array()) else {
            continue;
        };
        for item in series {
            let tag_id = item
                .pointer("/tags/tag_id")
                .and_then(|x| x.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let cols: Vec<String> = item
                .get("columns")
                .and_then(|c| c.as_array())
                .map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();
            let col = |name: &str| cols.iter().position(|c| c == name);
            let (Some(ti), Some(vi)) = (col("time"), col("value")) else {
                continue;
            };
            let qi = col("quality");
            let Some(values) = item.get("values").and_then(|x| x.as_array()) else {
                continue;
            };
            for row in values {
                let Some(cells) = row.as_array() else {
                    continue;
                };
                if ti >= cells.len() || vi >= cells.len() {
                    continue;
                }
                let ts = cells[ti]
                    .as_i64()
                    .or_else(|| cells[ti].as_f64().map(|f| f as i64))
                    .unwrap_or(0);
                let value = cells[vi].as_f64().unwrap_or(0.0);
                let quality = qi
                    .and_then(|i| cells.get(i))
                    .and_then(|x| x.as_f64())
                    .unwrap_or(0.0) as u16;
                out.push(Sample {
                    ts: DateTime::from_timestamp_millis(ts).unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap()),
                    tag_id,
                    value,
                    quality,
                    carried,
                });
            }
        }
    }
    Ok(out)
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
        self.ensure_dbrp().await
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
        self.query_last(tag_ids, at, false).await
    }

    async fn range(&self, tag_ids: &[u32], from: DateTime<Utc>, to: DateTime<Utc>) -> Result<Vec<Sample>> {
        let mut seed = self.query_last(tag_ids, from, true).await?;
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
