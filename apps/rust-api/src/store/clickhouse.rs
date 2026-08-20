use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use url::Url;

use super::{join_ids, Result, Store, StoreError};
use crate::model::{Sample, Tag};

pub struct ClickHouse {
    client: Client,
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct SampleRow {
    ts: i64,
    tag_id: u32,
    value: f32,
    quality: u16,
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct TagRow {
    id: u32,
    name: String,
    unit: String,
}

#[derive(Debug, Row, Deserialize)]
struct PingRow {
    #[allow(dead_code)]
    ok: u8,
}

impl ClickHouse {
    pub fn connect(url: &str, database: &str) -> Result<Self> {
        let parsed = Url::parse(url)?;
        let host = parsed.host_str().unwrap_or("clickhouse");
        let port = parsed.port().unwrap_or(8123);
        let origin = format!("{}://{host}:{port}", parsed.scheme());
        let mut client = Client::default().with_url(origin).with_database(database);
        if !parsed.username().is_empty() {
            client = client.with_user(parsed.username());
        }
        if let Some(password) = parsed.password() {
            client = client.with_password(password);
        }
        Ok(Self { client })
    }

    async fn locf_query(&self, tag_ids: &[u32], at: DateTime<Utc>, bounded: bool) -> Result<Vec<Sample>> {
        let bound = if bounded {
            format!(
                " AND ts >= toDateTime64('{}', 3, 'UTC') - INTERVAL 2 DAY",
                ch_time(at)
            )
        } else {
            String::new()
        };
        let sql = format!(
            r#"
            SELECT toUnixTimestamp64Milli(s.ts) AS ts, toUInt32(s.tag_id) AS tag_id,
                   toFloat32(s.value) AS value, toUInt16(s.quality) AS quality
            FROM samples AS s
            WHERE (s.tag_id, s.ts) IN (
                SELECT t.tag_id, max(t.ts)
                FROM samples AS t
                WHERE t.tag_id IN ({}) AND t.ts <= toDateTime64('{}', 3, 'UTC'){bound}
                GROUP BY t.tag_id
            )
            "#,
            join_ids(tag_ids),
            ch_time(at)
        );
        let rows = self
            .client
            .query(&sql)
            .fetch_all::<SampleRow>()
            .await
            .map_err(StoreError::new)?;
        Ok(rows
            .into_iter()
            .map(|r| Sample {
                ts: from_millis(r.ts),
                tag_id: r.tag_id,
                value: r.value as f64,
                quality: r.quality,
                carried: false,
            })
            .collect())
    }
}

fn ch_time(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

fn from_millis(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap())
}

fn missing_tag_ids(tag_ids: &[u32], rows: &[Sample]) -> Vec<u32> {
    let found: std::collections::HashSet<u32> = rows.iter().map(|s| s.tag_id).collect();
    let mut seen = std::collections::HashSet::new();
    let mut missing = Vec::new();
    for id in tag_ids {
        if !seen.insert(*id) {
            continue;
        }
        if !found.contains(id) {
            missing.push(*id);
        }
    }
    missing
}

fn merge_range(tag_ids: &[u32], head: Vec<Sample>, tail: Vec<Sample>) -> Vec<Sample> {
    use std::collections::HashMap;
    let mut buckets: HashMap<u32, Vec<Sample>> = HashMap::new();
    for id in tag_ids {
        buckets.entry(*id).or_default();
    }
    for sample in head {
        buckets.entry(sample.tag_id).or_default().push(sample);
    }
    let mut extra = Vec::new();
    for sample in tail {
        if let Some(bucket) = buckets.get_mut(&sample.tag_id) {
            bucket.push(sample);
        } else {
            extra.push(sample);
        }
    }
    let mut out = Vec::with_capacity(tag_ids.len());
    for id in tag_ids {
        if let Some(bucket) = buckets.get_mut(id) {
            out.append(bucket);
        }
    }
    out.extend(extra);
    out
}

#[async_trait]
impl Store for ClickHouse {
    fn name(&self) -> &'static str {
        "clickhouse"
    }

    async fn ping(&self) -> Result<()> {
        let _ = self
            .client
            .query("SELECT 1 AS ok")
            .fetch_one::<PingRow>()
            .await
            .map_err(StoreError::new)?;
        Ok(())
    }

    async fn write(&self, samples: &[Sample]) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }
        let mut insert = self.client.insert("samples").map_err(StoreError::new)?;
        for s in samples {
            insert
                .write(&SampleRow {
                    ts: s.ts.timestamp_millis(),
                    tag_id: s.tag_id,
                    value: s.value as f32,
                    quality: s.quality,
                })
                .await
                .map_err(StoreError::new)?;
        }
        insert.end().await.map_err(StoreError::new)?;
        Ok(())
    }

    async fn locf(&self, tag_ids: &[u32], at: DateTime<Utc>) -> Result<Vec<Sample>> {
        let mut out = self.locf_query(tag_ids, at, true).await?;
        let missing = missing_tag_ids(tag_ids, &out);
        if !missing.is_empty() {
            out.extend(self.locf_query(&missing, at, false).await?);
        }
        Ok(out)
    }

    async fn range(&self, tag_ids: &[u32], from: DateTime<Utc>, to: DateTime<Utc>) -> Result<Vec<Sample>> {
        let mut head = self.locf(tag_ids, from).await?;
        for sample in &mut head {
            sample.carried = true;
        }
        let sql = format!(
            r#"
            SELECT toUnixTimestamp64Milli(s.ts) AS ts, toUInt32(s.tag_id) AS tag_id,
                   toFloat32(s.value) AS value, toUInt16(s.quality) AS quality
            FROM samples AS s
            WHERE s.tag_id IN ({})
              AND s.ts > toDateTime64('{}', 3, 'UTC')
              AND s.ts <= toDateTime64('{}', 3, 'UTC')
            ORDER BY s.tag_id, s.ts
            "#,
            join_ids(tag_ids),
            ch_time(from),
            ch_time(to)
        );
        let rows = self
            .client
            .query(&sql)
            .fetch_all::<SampleRow>()
            .await
            .map_err(StoreError::new)?;
        let tail = rows
            .into_iter()
            .map(|r| Sample {
                ts: from_millis(r.ts),
                tag_id: r.tag_id,
                value: r.value as f64,
                quality: r.quality,
                carried: false,
            })
            .collect();
        Ok(merge_range(tag_ids, head, tail))
    }

    async fn upsert_tags(&self, tags: &[Tag]) -> Result<()> {
        if tags.is_empty() {
            return Ok(());
        }
        let mut insert = self.client.insert("tags").map_err(StoreError::new)?;
        for t in tags {
            insert
                .write(&TagRow {
                    id: t.id,
                    name: t.name.clone(),
                    unit: t.unit.clone(),
                })
                .await
                .map_err(StoreError::new)?;
        }
        insert.end().await.map_err(StoreError::new)?;
        Ok(())
    }

    async fn list_tags(&self) -> Result<Vec<Tag>> {
        let rows = self
            .client
            .query("SELECT toUInt32(id) AS id, name, unit FROM tags ORDER BY id")
            .fetch_all::<TagRow>()
            .await
            .map_err(StoreError::new)?;
        Ok(rows
            .into_iter()
            .map(|r| Tag {
                id: r.id,
                name: r.name,
                unit: r.unit,
            })
            .collect())
    }
}
