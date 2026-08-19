use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clickhouse::{Client, Row};
use serde::Deserialize;
use url::Url;

use super::{join_ids, Result, Store, StoreError};
use crate::model::{Sample, Tag};

pub struct ClickHouse {
    client: Client,
}

#[derive(Debug, Row, Deserialize)]
struct SampleRow {
    ts: i64,
    tag_id: u32,
    value: f32,
    quality: u16,
}

#[derive(Debug, Row, Deserialize)]
struct SampleRowCarried {
    ts: i64,
    tag_id: u32,
    value: f32,
    quality: u16,
    carried: u8,
}

#[derive(Debug, Row, Deserialize)]
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
}

fn ch_time(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

fn from_millis(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap())
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
        let mut sql = String::from("INSERT INTO samples (ts, tag_id, value, quality) VALUES ");
        for (i, s) in samples.iter().enumerate() {
            if i > 0 {
                sql.push(',');
            }
            sql.push_str(&format!(
                "(fromUnixTimestamp64Milli({}), {}, {}, {})",
                s.ts.timestamp_millis(),
                s.tag_id,
                s.value as f32,
                s.quality
            ));
        }
        self.client.query(&sql).execute().await.map_err(StoreError::new)?;
        Ok(())
    }

    async fn locf(&self, tag_ids: &[u32], at: DateTime<Utc>) -> Result<Vec<Sample>> {
        let sql = format!(
            r#"
            SELECT toUnixTimestamp64Milli(ts) AS ts, toUInt32(tag_id) AS tag_id,
                   toFloat32(value) AS value, toUInt16(quality) AS quality
            FROM samples
            WHERE tag_id IN ({}) AND ts <= toDateTime64('{}', 3, 'UTC')
            ORDER BY tag_id, ts DESC
            LIMIT 1 BY tag_id
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

    async fn range(&self, tag_ids: &[u32], from: DateTime<Utc>, to: DateTime<Utc>) -> Result<Vec<Sample>> {
        let ids = join_ids(tag_ids);
        let sql = format!(
            r#"
            SELECT ts, tag_id, value, quality, carried FROM (
                SELECT toUnixTimestamp64Milli(ts) AS ts, toUInt32(tag_id) AS tag_id,
                       toFloat32(value) AS value, toUInt16(quality) AS quality, 1 AS carried
                FROM samples
                WHERE tag_id IN ({ids}) AND ts <= toDateTime64('{}', 3, 'UTC')
                ORDER BY tag_id, ts DESC
                LIMIT 1 BY tag_id
                UNION ALL
                SELECT toUnixTimestamp64Milli(ts) AS ts, toUInt32(tag_id) AS tag_id,
                       toFloat32(value) AS value, toUInt16(quality) AS quality, 0
                FROM samples
                WHERE tag_id IN ({ids})
                  AND ts > toDateTime64('{}', 3, 'UTC')
                  AND ts <= toDateTime64('{}', 3, 'UTC')
            )
            ORDER BY tag_id, ts
            "#,
            ch_time(from),
            ch_time(from),
            ch_time(to)
        );
        let rows = self
            .client
            .query(&sql)
            .fetch_all::<SampleRowCarried>()
            .await
            .map_err(StoreError::new)?;
        Ok(rows
            .into_iter()
            .map(|r| Sample {
                ts: from_millis(r.ts),
                tag_id: r.tag_id,
                value: r.value as f64,
                quality: r.quality,
                carried: r.carried != 0,
            })
            .collect())
    }

    async fn upsert_tags(&self, tags: &[Tag]) -> Result<()> {
        if tags.is_empty() {
            return Ok(());
        }
        let mut sql = String::from("INSERT INTO tags (id, name, unit) VALUES ");
        for (i, t) in tags.iter().enumerate() {
            if i > 0 {
                sql.push(',');
            }
            sql.push_str(&format!(
                "({}, '{}', '{}')",
                t.id,
                t.name.replace('\'', "''"),
                t.unit.replace('\'', "''")
            ));
        }
        self.client.query(&sql).execute().await.map_err(StoreError::new)?;
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
