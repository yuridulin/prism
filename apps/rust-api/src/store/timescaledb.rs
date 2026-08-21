use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Write as _;

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
struct TagRow {
    id: i32,
    name: String,
    unit: String,
}

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
    let mut out = Vec::new();
    for id in tag_ids {
        if let Some(bucket) = buckets.get_mut(id) {
            out.append(bucket);
        }
    }
    out.extend(extra);
    out
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
            .copy_in_raw("COPY samples (ts, tag_id, value, quality) FROM STDIN")
            .await?;
        let mut buf = String::with_capacity(samples.len() * 48);
        for s in samples {
            let _ = write!(
                buf,
                "{}\t{}\t{}\t{}\n",
                s.ts.format("%Y-%m-%d %H:%M:%S%.6f+00"),
                s.tag_id as i32,
                s.value as f32,
                s.quality as i16
            );
        }
        copy.send(buf.as_bytes()).await?;
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
        let mut head = self.locf(tag_ids, from).await?;
        for sample in &mut head {
            sample.carried = true;
        }
        let ids: Vec<i32> = tag_ids.iter().map(|&id| id as i32).collect();
        let rows = sqlx::query_as::<_, SampleRow>(
            r#"
            SELECT ts, tag_id, value, quality
            FROM samples
            WHERE tag_id = ANY($1) AND ts > $2 AND ts <= $3
            ORDER BY tag_id, ts
            "#,
        )
        .bind(&ids)
        .bind(from)
        .bind(to)
        .fetch_all(&self.pool)
        .await?;
        let tail = rows.into_iter().map(|r| to_sample(r, false)).collect();
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
