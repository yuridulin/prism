use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Mutex;
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt;

use crate::metrics;
use crate::model::{Sample, Tag};

mod clickhouse;
mod influx;
mod questdb;
mod timescaledb;
mod victoriametrics;

pub use clickhouse::ClickHouse;
pub use influx::Influx;
pub use questdb::QuestDb;
pub use timescaledb::Timescale;
pub use victoriametrics::VictoriaMetrics;

#[derive(Debug)]
pub struct StoreError(pub String);

impl StoreError {
    pub fn new(msg: impl std::fmt::Display) -> Self {
        Self(msg.to_string())
    }
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for StoreError {}

impl From<reqwest::Error> for StoreError {
    fn from(err: reqwest::Error) -> Self {
        Self(err.to_string())
    }
}

impl From<sqlx::Error> for StoreError {
    fn from(err: sqlx::Error) -> Self {
        Self(err.to_string())
    }
}

impl From<url::ParseError> for StoreError {
    fn from(err: url::ParseError) -> Self {
        Self(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, StoreError>;

#[async_trait]
pub trait Store: Send + Sync {
    fn name(&self) -> &'static str;
    async fn ping(&self) -> Result<()>;
    async fn write(&self, samples: &[Sample]) -> Result<()>;
    async fn locf(&self, tag_ids: &[u32], at: DateTime<Utc>) -> Result<Vec<Sample>>;
    async fn range(&self, tag_ids: &[u32], from: DateTime<Utc>, to: DateTime<Utc>) -> Result<Vec<Sample>>;
    async fn upsert_tags(&self, tags: &[Tag]) -> Result<()>;
    async fn list_tags(&self) -> Result<Vec<Tag>>;
}

pub struct Observed {
    inner: Box<dyn Store>,
}

impl Observed {
    pub fn new(inner: Box<dyn Store>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl Store for Observed {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    async fn ping(&self) -> Result<()> {
        let start = Instant::now();
        let res = self.inner.ping().await;
        metrics::observe_storage(self.name(), "ping", start.elapsed(), res.is_err());
        res
    }

    async fn write(&self, samples: &[Sample]) -> Result<()> {
        let start = Instant::now();
        let res = self.inner.write(samples).await;
        metrics::observe_storage(self.name(), "write", start.elapsed(), res.is_err());
        res
    }

    async fn locf(&self, tag_ids: &[u32], at: DateTime<Utc>) -> Result<Vec<Sample>> {
        let start = Instant::now();
        let res = self.inner.locf(tag_ids, at).await;
        metrics::observe_storage(self.name(), "locf", start.elapsed(), res.is_err());
        res
    }

    async fn range(&self, tag_ids: &[u32], from: DateTime<Utc>, to: DateTime<Utc>) -> Result<Vec<Sample>> {
        let start = Instant::now();
        let res = self.inner.range(tag_ids, from, to).await;
        metrics::observe_storage(self.name(), "range", start.elapsed(), res.is_err());
        res
    }

    async fn upsert_tags(&self, tags: &[Tag]) -> Result<()> {
        let start = Instant::now();
        let res = self.inner.upsert_tags(tags).await;
        metrics::observe_storage(self.name(), "tags", start.elapsed(), res.is_err());
        res
    }

    async fn list_tags(&self) -> Result<Vec<Tag>> {
        let start = Instant::now();
        let res = self.inner.list_tags().await;
        metrics::observe_storage(self.name(), "tags", start.elapsed(), res.is_err());
        res
    }
}

#[derive(Default)]
pub struct Catalog {
    data: Mutex<HashMap<u32, Tag>>,
}

impl Catalog {
    pub fn upsert(&self, tags: &[Tag]) {
        let mut data = self.data.lock().expect("catalog lock");
        for t in tags {
            data.insert(t.id, t.clone());
        }
    }

    pub fn list(&self) -> Vec<Tag> {
        let data = self.data.lock().expect("catalog lock");
        let mut out: Vec<Tag> = data.values().cloned().collect();
        out.sort_by_key(|t| t.id);
        out
    }
}

pub fn join_ids(ids: &[u32]) -> String {
    ids.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(",")
}

/// ILP requires a decimal point on whole floats (`1.0`, not `1`).
pub fn append_ilp_float(buf: &mut String, v: f64) {
    if v.is_finite() && v.fract() == 0.0 {
        let _ = write!(buf, "{v:.1}");
    } else {
        let _ = write!(buf, "{v}");
    }
}

pub fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .pool_max_idle_per_host(32)
        .tcp_nodelay(true)
        .http1_only()
        .build()
        .expect("http client")
}

/// Call `on_line` for each complete CSV/text line from an HTTP body, without buffering the whole payload as a String.
pub async fn foreach_line(
    resp: reqwest::Response,
    mut on_line: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    let mut stream = resp.bytes_stream();
    let mut buf: Vec<u8> = Vec::with_capacity(64 << 10);
    while let Some(chunk) = stream.next().await {
        buf.extend_from_slice(&chunk?);
        let mut start = 0;
        while let Some(rel) = buf[start..].iter().position(|&b| b == b'\n') {
            let end = start + rel;
            let line = trim_cr(&buf[start..end]);
            let text = std::str::from_utf8(line).map_err(|e| StoreError::new(e))?;
            on_line(text)?;
            start = end + 1;
        }
        if start > 0 {
            buf.drain(..start);
        }
    }
    if !buf.is_empty() {
        let line = trim_cr(&buf);
        let text = std::str::from_utf8(line).map_err(|e| StoreError::new(e))?;
        if !text.is_empty() {
            on_line(text)?;
        }
    }
    Ok(())
}

fn trim_cr(line: &[u8]) -> &[u8] {
    line.strip_suffix(&[b'\r']).unwrap_or(line)
}

pub async fn open(kind: &str, cfg: &crate::Config) -> Result<Observed> {
    let inner: Box<dyn Store> = match kind {
        "timescaledb" => Box::new(Timescale::connect(&cfg.postgres_dsn).await?),
        "clickhouse" => Box::new(ClickHouse::connect(&cfg.clickhouse_url, &cfg.clickhouse_db)?),
        "questdb" => Box::new(QuestDb::new(&cfg.questdb_url, &cfg.questdb_ilp)),
        "influxdb" => Box::new(Influx::new(
            &cfg.influx_url,
            &cfg.influx_token,
            &cfg.influx_org,
            &cfg.influx_bucket,
        )),
        "victoriametrics" => Box::new(VictoriaMetrics::new(&cfg.vm_url)),
        other => return Err(StoreError::new(format!("unsupported storage {other}"))),
    };
    Ok(Observed::new(inner))
}
