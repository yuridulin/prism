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
struct SampleRowCarried {
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

impl Timescale {
    pub async fn connect(dsn: &str) -> Result<Self> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(16)
            .connect(dsn)
            .await
            .map_err(StoreError::from)?;
        Ok(Self { pool })
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
        let mut ts = Vec::with_capacity(samples.len());
        let mut tag_ids = Vec::with_capacity(samples.len());
        let mut values = Vec::with_capacity(samples.len());
        let mut qualities = Vec::with_capacity(samples.len());
        for s in samples {
            ts.push(s.ts);
            tag_ids.push(s.tag_id as i32);
            values.push(s.value as f32);
            qualities.push(s.quality as i16);
        }
        sqlx::query(
            r#"
            INSERT INTO samples (ts, tag_id, value, quality)
            SELECT * FROM UNNEST($1::timestamptz[], $2::int4[], $3::float4[], $4::int2[])
            "#,
        )
        .bind(&ts)
        .bind(&tag_ids)
        .bind(&values)
        .bind(&qualities)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn locf(&self, tag_ids: &[u32], at: DateTime<Utc>) -> Result<Vec<Sample>> {
        let ids: Vec<i32> = tag_ids.iter().map(|&id| id as i32).collect();
        let rows = sqlx::query_as::<_, SampleRow>(
            r#"
            SELECT DISTINCT ON (tag_id) ts, tag_id, value, quality
            FROM samples
            WHERE tag_id = ANY($1) AND ts <= $2
            ORDER BY tag_id, ts DESC
            "#,
        )
        .bind(&ids)
        .bind(at)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| to_sample(r, false)).collect())
    }

    async fn range(&self, tag_ids: &[u32], from: DateTime<Utc>, to: DateTime<Utc>) -> Result<Vec<Sample>> {
        let ids: Vec<i32> = tag_ids.iter().map(|&id| id as i32).collect();
        let rows = sqlx::query_as::<_, SampleRowCarried>(
            r#"
            SELECT ts, tag_id, value, quality, carried FROM (
                SELECT DISTINCT ON (tag_id) ts, tag_id, value, quality, true AS carried
                FROM samples
                WHERE tag_id = ANY($1) AND ts <= $2
                ORDER BY tag_id, ts DESC
                UNION ALL
                SELECT ts, tag_id, value, quality, false
                FROM samples
                WHERE tag_id = ANY($1) AND ts > $2 AND ts <= $3
            ) s
            ORDER BY tag_id, ts
            "#,
        )
        .bind(&ids)
        .bind(from)
        .bind(to)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| Sample {
                ts: r.ts,
                tag_id: r.tag_id as u32,
                value: r.value as f64,
                quality: r.quality as u16,
                carried: r.carried,
            })
            .collect())
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
