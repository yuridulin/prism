use std::collections::HashMap;
use std::collections::HashSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};

use super::{Result, Store, StoreError};
use crate::model::{Sample, Tag};

pub struct Timescale {
    pool: PgPool,
}

#[derive(FromRow)]
struct SampleRow {
    ts: DateTime<Utc>,
    tag_id: i32,
    value: f32,
    quality: i16,
}

#[derive(FromRow)]
struct RangeRow {
    ts: DateTime<Utc>,
    tag_id: i32,
    value: f32,
    quality: i16,
    carried: bool,
}

#[derive(FromRow)]
struct TagRow {
    id: i32,
    name: String,
    unit: String,
}

const PGCOPY_SIG: &[u8] = b"PGCOPY\n\xff\r\n\0";
const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800 * 1_000_000;

impl Timescale {
    pub async fn connect(dsn: &str) -> Result<Self> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(16)
            .connect(dsn)
            .await
            .map_err(StoreError::from)?;
        Ok(Self { pool })
    }

    async fn locf_query(&self, tag_ids: &[u32], at: DateTime<Utc>, bounded: bool) -> Result<Vec<Sample>> {
        let ids: Vec<i32> = tag_ids.iter().map(|&id| id as i32).collect();
        let rows = if bounded {
            let since = at - chrono::Duration::hours(3);
            sqlx::query_as::<_, SampleRow>(
                r#"
                SELECT s.ts, s.tag_id, s.value, s.quality
                FROM unnest($1::int4[]) AS t(tag_id)
                CROSS JOIN LATERAL (
                    SELECT ts, tag_id, value, quality
                    FROM samples
                    WHERE samples.tag_id = t.tag_id AND ts <= $2 AND ts >= $3
                    ORDER BY ts DESC
                    LIMIT 1
                ) s
                "#,
            )
            .bind(&ids)
            .bind(at)
            .bind(since)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, SampleRow>(
                r#"
                SELECT s.ts, s.tag_id, s.value, s.quality
                FROM unnest($1::int4[]) AS t(tag_id)
                CROSS JOIN LATERAL (
                    SELECT ts, tag_id, value, quality
                    FROM samples
                    WHERE samples.tag_id = t.tag_id AND ts <= $2
                    ORDER BY ts DESC
                    LIMIT 1
                ) s
                "#,
            )
            .bind(&ids)
            .bind(at)
            .fetch_all(&self.pool)
            .await?
        };
        Ok(rows.into_iter().map(|r| to_sample(r, false)).collect())
    }
}

fn to_sample(row: SampleRow, carried: bool) -> Sample {
    Sample {
        ts: row.ts,
        tag_id: row.tag_id as u32,
        value: row.value as f64,
        quality: row.quality as u16,
        carried,
    }
}

fn merge_range(tag_ids: &[u32], head: Vec<Sample>, tail: Vec<Sample>) -> Vec<Sample> {
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
    let mut out = Vec::with_capacity(tag_ids.len() + extra.len());
    for id in tag_ids {
        if let Some(bucket) = buckets.get_mut(id) {
            out.append(bucket);
        }
    }
    out.extend(extra);
    out
}

fn encode_copy_binary(samples: &[Sample]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(19 + samples.len() * 36 + 2);
    buf.extend_from_slice(PGCOPY_SIG);
    buf.extend_from_slice(&0i32.to_be_bytes());
    buf.extend_from_slice(&0i32.to_be_bytes());
    for s in samples {
        buf.extend_from_slice(&4i16.to_be_bytes());
        buf.extend_from_slice(&8i32.to_be_bytes());
        let micros = s.ts.timestamp_micros() - PG_EPOCH_UNIX_MICROS;
        buf.extend_from_slice(&micros.to_be_bytes());
        buf.extend_from_slice(&4i32.to_be_bytes());
        buf.extend_from_slice(&(s.tag_id as i32).to_be_bytes());
        buf.extend_from_slice(&4i32.to_be_bytes());
        buf.extend_from_slice(&(s.value as f32).to_bits().to_be_bytes());
        buf.extend_from_slice(&2i32.to_be_bytes());
        buf.extend_from_slice(&(s.quality as i16).to_be_bytes());
    }
    buf.extend_from_slice(&(-1i16).to_be_bytes());
    buf
}

#[async_trait]
impl Store for Timescale {
    fn name(&self) -> &'static str {
        "timescaledb"
    }

    async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    async fn write(&self, samples: &[Sample]) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }
        let mut conn = self.pool.acquire().await?;
        let mut copy = conn
            .copy_in_raw("COPY samples (ts, tag_id, value, quality) FROM STDIN WITH (FORMAT BINARY)")
            .await?;
        copy.send(encode_copy_binary(samples)).await?;
        copy.finish().await?;
        Ok(())
    }

    async fn locf(&self, tag_ids: &[u32], at: DateTime<Utc>) -> Result<Vec<Sample>> {
        let mut out = self.locf_query(tag_ids, at, true).await?;
        let found: HashSet<u32> = out.iter().map(|s| s.tag_id).collect();
        let missing: Vec<u32> = tag_ids.iter().copied().filter(|id| !found.contains(id)).collect();
        if !missing.is_empty() {
            out.extend(self.locf_query(&missing, at, false).await?);
        }
        Ok(out)
    }

    async fn range(&self, tag_ids: &[u32], from: DateTime<Utc>, to: DateTime<Utc>) -> Result<Vec<Sample>> {
        let ids: Vec<i32> = tag_ids.iter().map(|&id| id as i32).collect();
        let rows = sqlx::query_as::<_, RangeRow>(
            r#"
            SELECT s.ts, s.tag_id, s.value, s.quality, true AS carried
            FROM unnest($1::int4[]) AS t(tag_id)
            CROSS JOIN LATERAL (
                SELECT ts, tag_id, value, quality
                FROM samples
                WHERE samples.tag_id = t.tag_id AND ts <= $2
                ORDER BY ts DESC
                LIMIT 1
            ) s
            UNION ALL
            SELECT ts, tag_id, value, quality, false
            FROM samples
            WHERE tag_id = ANY($1) AND ts > $2 AND ts <= $3
            "#,
        )
        .bind(&ids)
        .bind(from)
        .bind(to)
        .fetch_all(&self.pool)
        .await?;
        let mut head = Vec::with_capacity(tag_ids.len());
        let mut tail = Vec::with_capacity(rows.len());
        for r in rows {
            let sample = Sample {
                ts: r.ts,
                tag_id: r.tag_id as u32,
                value: r.value as f64,
                quality: r.quality as u16,
                carried: r.carried,
            };
            if r.carried {
                head.push(sample);
            } else {
                tail.push(sample);
            }
        }
        Ok(merge_range(tag_ids, head, tail))
    }

    async fn upsert_tags(&self, tags: &[Tag]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for t in tags {
            sqlx::query(
                r#"
                INSERT INTO tags (id, name, unit) VALUES ($1, $2, $3)
                ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, unit = EXCLUDED.unit
                "#,
            )
            .bind(t.id as i32)
            .bind(&t.name)
            .bind(&t.unit)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn list_tags(&self) -> Result<Vec<Tag>> {
        let rows = sqlx::query_as::<_, TagRow>("SELECT id, name, COALESCE(unit, '') AS unit FROM tags ORDER BY id")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| Tag {
                id: r.id as u32,
                name: r.name,
                unit: r.unit,
            })
            .collect())
    }
}
