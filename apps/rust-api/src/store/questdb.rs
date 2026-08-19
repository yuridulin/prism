use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use url::Url;

use super::{http_client, join_ids, Result, Store, StoreError};
use crate::model::{Sample, Tag};

pub struct QuestDb {
    base: String,
    client: reqwest::Client,
}

#[derive(Debug, Deserialize)]
struct QdbExec {
    #[serde(default)]
    dataset: Vec<Vec<serde_json::Value>>,
    #[serde(default)]
    error: String,
}

impl QuestDb {
    pub fn new(http_url: &str) -> Self {
        Self {
            base: http_url.trim_end_matches('/').to_string(),
            client: http_client(),
        }
    }

    async fn exec(&self, query: &str) -> Result<QdbExec> {
        let mut u = Url::parse(&format!("{}/exec", self.base))?;
        u.query_pairs_mut().append_pair("query", query);
        let resp = self.client.get(u).send().await?;
        let out: QdbExec = resp.json().await?;
        if !out.error.is_empty() {
            return Err(StoreError::new(format!("questdb: {}", out.error)));
        }
        Ok(out)
    }
}

fn qdb_time(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}

fn as_f64(v: &serde_json::Value) -> f64 {
    match v {
        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
        serde_json::Value::String(s) => s.parse().unwrap_or(0.0),
        serde_json::Value::Bool(b) => {
            if *b {
                1.0
            } else {
                0.0
            }
        }
        _ => 0.0,
    }
}

fn as_bool(v: &serde_json::Value) -> bool {
    match v {
        serde_json::Value::Bool(b) => *b,
        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0) != 0.0,
        serde_json::Value::String(s) => matches!(s.as_str(), "true" | "t" | "1"),
        _ => false,
    }
}

fn parse_qdb_ts(v: &serde_json::Value) -> Result<DateTime<Utc>> {
    match v {
        serde_json::Value::String(s) => {
            if let Ok(n) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.fZ") {
                return Ok(n.and_utc());
            }
            DateTime::parse_from_rfc3339(s)
                .map(|t| t.with_timezone(&Utc))
                .map_err(|_| StoreError::new(format!("questdb ts {s}")))
        }
        serde_json::Value::Number(n) => {
            let ms = n.as_f64().unwrap_or(0.0) as i64;
            DateTime::from_timestamp_millis(ms).ok_or_else(|| StoreError::new("questdb ts millis"))
        }
        other => Err(StoreError::new(format!("questdb ts {other}"))),
    }
}

fn parse_samples(data: QdbExec, has_carried: bool) -> Result<Vec<Sample>> {
    let mut out = Vec::new();
    for row in data.dataset {
        if row.len() < 4 {
            continue;
        }
        let ts = parse_qdb_ts(&row[0])?;
        let mut s = Sample {
            ts,
            tag_id: as_f64(&row[1]) as u32,
            value: as_f64(&row[2]),
            quality: as_f64(&row[3]) as u16,
            carried: false,
        };
        if has_carried && row.len() > 4 {
            s.carried = as_bool(&row[4]);
        }
        out.push(s);
    }
    Ok(out)
}

fn ilp_float(v: f64) -> String {
    if v.fract() == 0.0 {
        format!("{v:.1}")
    } else {
        format!("{v}")
    }
}

#[async_trait]
impl Store for QuestDb {
    fn name(&self) -> &'static str {
        "questdb"
    }

    async fn ping(&self) -> Result<()> {
        self.exec("SELECT 1").await?;
        Ok(())
    }

    async fn write(&self, samples: &[Sample]) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }
        let mut body = String::new();
        for p in samples {
            body.push_str(&format!(
                "samples tag_id={}i,value={},quality={}i {}\n",
                p.tag_id,
                ilp_float(p.value),
                p.quality,
                p.ts.timestamp_nanos_opt().unwrap_or(0)
            ));
        }
        let resp = self
            .client
            .post(format!("{}/write?precision=n", self.base))
            .header("Content-Type", "text/plain")
            .body(body)
            .send()
            .await?;
        let status = resp.status();
        if status.as_u16() >= 300 {
            let text = resp.text().await.unwrap_or_default();
            return Err(StoreError::new(format!("questdb write {status}: {text}")));
        }
        Ok(())
    }

    async fn locf(&self, tag_ids: &[u32], at: DateTime<Utc>) -> Result<Vec<Sample>> {
        let q = format!(
            "SELECT ts, tag_id, value, quality FROM samples WHERE tag_id IN ({}) AND ts <= '{}' LATEST ON ts PARTITION BY tag_id",
            join_ids(tag_ids),
            qdb_time(at)
        );
        parse_samples(self.exec(&q).await?, false)
    }

    async fn range(&self, tag_ids: &[u32], from: DateTime<Utc>, to: DateTime<Utc>) -> Result<Vec<Sample>> {
        let ids = join_ids(tag_ids);
        let q = format!(
            r#"
            SELECT ts, tag_id, value, quality, carried FROM (
                SELECT ts, tag_id, value, quality, true AS carried
                FROM samples
                WHERE tag_id IN ({ids}) AND ts <= '{}'
                LATEST ON ts PARTITION BY tag_id
                UNION ALL
                SELECT ts, tag_id, value, quality, false
                FROM samples
                WHERE tag_id IN ({ids}) AND ts > '{}' AND ts <= '{}'
            )
            ORDER BY tag_id, ts
            "#,
            qdb_time(from),
            qdb_time(from),
            qdb_time(to)
        );
        parse_samples(self.exec(&q).await?, true)
    }

    async fn upsert_tags(&self, tags: &[Tag]) -> Result<()> {
        for t in tags {
            let name = t.name.replace('\'', "''");
            let unit = t.unit.replace('\'', "''");
            let q = format!(
                "INSERT INTO tags (id, name, unit) VALUES ({}, '{name}', '{unit}')",
                t.id
            );
            self.exec(&q).await?;
        }
        Ok(())
    }

    async fn list_tags(&self) -> Result<Vec<Tag>> {
        let data = self.exec("SELECT id, name, unit FROM tags ORDER BY id").await?;
        let mut out = Vec::new();
        for row in data.dataset {
            if row.len() < 3 {
                continue;
            }
            out.push(Tag {
                id: as_f64(&row[0]) as u32,
                name: json_string(&row[1]),
                unit: json_string(&row[2]),
            });
        }
        Ok(out)
    }
}

fn json_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string().trim_matches('"').to_string(),
    }
}
