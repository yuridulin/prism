use std::fmt::Write as _;
use std::fmt;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::Deserialize;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use url::Url;

use super::{append_ilp_float, foreach_line, http_client, Result, Store, StoreError};
use crate::model::{Sample, Tag};

pub struct QuestDb {
    base: String,
    ilp_addr: String,
    client: reqwest::Client,
    pool: Mutex<Vec<TcpStream>>,
}

#[derive(Debug, Deserialize)]
struct QdbExec {
    #[serde(default)]
    dataset: Vec<Vec<serde_json::Value>>,
    #[serde(default)]
    error: String,
}

#[derive(Deserialize)]
struct QdbSampleExec {
    #[serde(default)]
    dataset: Vec<QdbSample>,
    #[serde(default)]
    error: String,
}

struct QdbSample(Sample);

impl QuestDb {
    pub fn new(http_url: &str, ilp_addr: &str) -> Self {
        let ilp_addr = ilp_addr
            .trim()
            .trim_start_matches("tcp://")
            .trim_start_matches("ilp://")
            .to_string();
        Self {
            base: http_url.trim_end_matches('/').to_string(),
            ilp_addr,
            client: http_client(),
            pool: Mutex::new(Vec::new()),
        }
    }

    async fn send_ilp(&self, body: &[u8]) -> Result<()> {
        match self.send_ilp_once(body).await {
            Ok(()) => Ok(()),
            Err(_) => self.send_ilp_once(body).await,
        }
    }

    async fn send_ilp_once(&self, body: &[u8]) -> Result<()> {
        let mut stream = {
            let mut pool = self.pool.lock().await;
            pool.pop()
        };
        if stream.is_none() {
            let created = TcpStream::connect(&self.ilp_addr)
                .await
                .map_err(StoreError::new)?;
            let _ = created.set_nodelay(true);
            stream = Some(created);
        }
        let mut stream = stream.expect("ilp stream");
        if let Err(err) = stream.write_all(body).await {
            return Err(StoreError::new(err));
        }
        if let Err(err) = stream.flush().await {
            return Err(StoreError::new(err));
        }
        let mut pool = self.pool.lock().await;
        if pool.len() < 8 {
            pool.push(stream);
        }
        Ok(())
    }

    async fn exec(&self, query: &str) -> Result<QdbExec> {
        Ok(self.exec_json(query).await?)
    }

    async fn exec_samples(&self, query: &str) -> Result<Vec<Sample>> {
        let out: QdbSampleExec = self.exec_json(query).await?;
        Ok(out.dataset.into_iter().map(|row| row.0).collect())
    }

    async fn exp_samples(&self, query: &str) -> Result<Vec<Sample>> {
        let mut u = Url::parse(&format!("{}/exp", self.base))?;
        u.query_pairs_mut().append_pair("query", query);
        let resp = self.client.get(u).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(StoreError::new(format!(
                "questdb exp {status}: {}",
                text.chars().take(500).collect::<String>()
            )));
        }
        parse_qdb_csv_stream(resp).await
    }

    async fn exec_json<T>(&self, query: &str) -> Result<T>
    where
        T: for<'de> Deserialize<'de> + QdbError,
    {
        let mut u = Url::parse(&format!("{}/exec", self.base))?;
        u.query_pairs_mut().append_pair("query", query);
        let bytes = self.client.get(u).send().await?.bytes().await?;
        let out: T = serde_json::from_slice(&bytes).map_err(StoreError::new)?;
        if let Some(err) = out.qdb_error() {
            return Err(StoreError::new(format!("questdb: {err}")));
        }
        Ok(out)
    }
}

trait QdbError {
    fn qdb_error(&self) -> Option<&str>;
}

impl QdbError for QdbExec {
    fn qdb_error(&self) -> Option<&str> {
        if self.error.is_empty() {
            None
        } else {
            Some(&self.error)
        }
    }
}

impl QdbError for QdbSampleExec {
    fn qdb_error(&self) -> Option<&str> {
        if self.error.is_empty() {
            None
        } else {
            Some(&self.error)
        }
    }
}

fn qdb_time(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}

fn join_symbol_ids(ids: &[u32]) -> String {
    ids.iter()
        .map(|id| format!("'{id}'"))
        .collect::<Vec<_>>()
        .join(",")
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

struct CsvCols {
    ts: usize,
    tag: usize,
    val: usize,
    quality: usize,
    carried: Option<usize>,
}

async fn parse_qdb_csv_stream(resp: reqwest::Response) -> Result<Vec<Sample>> {
    let mut cols: Option<CsvCols> = None;
    let mut out = Vec::with_capacity(4096);
    foreach_line(resp, |line| {
        if line.is_empty() {
            return Ok(());
        }
        if cols.is_none() {
            cols = Some(parse_qdb_header(line)?);
            return Ok(());
        }
        if let Some(sample) = parse_qdb_csv_row(cols.as_ref().unwrap(), line) {
            out.push(sample);
        }
        Ok(())
    })
    .await?;
    Ok(out)
}

fn parse_qdb_header(header: &str) -> Result<CsvCols> {
    let names: Vec<&str> = header.split(',').map(|s| s.trim().trim_matches('"')).collect();
    let idx = |name: &str| {
        names
            .iter()
            .position(|c| c.eq_ignore_ascii_case(name))
    };
    let (Some(ts), Some(tag), Some(val), Some(quality)) =
        (idx("ts"), idx("tag_id"), idx("value"), idx("quality"))
    else {
        return Err(StoreError::new(format!("questdb csv columns {header}")));
    };
    Ok(CsvCols {
        ts,
        tag,
        val,
        quality,
        carried: idx("carried"),
    })
}

fn parse_qdb_csv_row(cols: &CsvCols, line: &str) -> Option<Sample> {
    let mut fields = [""; 8];
    let n = split_csv(line, &mut fields);
    if cols.ts >= n || cols.tag >= n || cols.val >= n || cols.quality >= n {
        return None;
    }
    let ts = parse_qdb_ts_str(fields[cols.ts].trim().trim_matches('"')).ok()?;
    let tag_id: u32 = fields[cols.tag].trim_matches('"').parse().unwrap_or(0);
    let value: f64 = fields[cols.val].parse().unwrap_or(0.0);
    let quality: u16 = fields[cols.quality].parse::<f64>().unwrap_or(0.0) as u16;
    let carried = cols
        .carried
        .filter(|&i| i < n)
        .map(|i| matches!(fields[i], "true" | "t" | "1"))
        .unwrap_or(false);
    Some(Sample {
        ts,
        tag_id,
        value,
        quality,
        carried,
    })
}

fn split_csv<'a>(line: &'a str, out: &mut [&'a str]) -> usize {
    let mut n = 0;
    let mut start = 0;
    let bytes = line.as_bytes();
    for i in 0..bytes.len() {
        if bytes[i] == b',' {
            if n < out.len() {
                out[n] = &line[start..i];
                n += 1;
            }
            start = i + 1;
        }
    }
    if n < out.len() {
        out[n] = &line[start..];
        n += 1;
    }
    n
}

fn parse_qdb_ts_str(s: &str) -> std::result::Result<DateTime<Utc>, ()> {
    let s = s.trim();
    if let Ok(n) = s.parse::<i64>() {
        if n > 10_000_000_000_000 {
            return DateTime::from_timestamp_micros(n).ok_or(());
        }
        if n > 10_000_000_000 {
            return DateTime::from_timestamp_millis(n).ok_or(());
        }
        return DateTime::from_timestamp(n, 0).ok_or(());
    }
    const FMTS: &[&str] = &[
        "%Y-%m-%dT%H:%M:%S%.6fZ",
        "%Y-%m-%dT%H:%M:%S%.fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%.6f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.6f",
        "%Y-%m-%d %H:%M:%S",
    ];
    for fmt in FMTS {
        if let Ok(n) = NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(n.and_utc());
        }
    }
    DateTime::parse_from_rfc3339(s)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|_| ())
}

impl<'de> Deserialize<'de> for QdbSample {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        deserializer.deserialize_seq(QdbSampleVisitor)
    }
}

struct QdbSampleVisitor;

impl<'de> Visitor<'de> for QdbSampleVisitor {
    type Value = QdbSample;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("questdb sample row")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<QdbSample, A::Error> {
        let ts = match seq.next_element::<TsCell>()? {
            Some(cell) => cell.0,
            None => return Err(de::Error::invalid_length(0, &self)),
        };
        let tag_id = match seq.next_element::<U32Cell>()? {
            Some(cell) => cell.0,
            None => return Err(de::Error::invalid_length(1, &self)),
        };
        let value = match seq.next_element::<F64Cell>()? {
            Some(cell) => cell.0,
            None => return Err(de::Error::invalid_length(2, &self)),
        };
        let quality = match seq.next_element::<U32Cell>()? {
            Some(cell) => cell.0 as u16,
            None => return Err(de::Error::invalid_length(3, &self)),
        };
        let carried = seq.next_element::<BoolCell>()?.map(|c| c.0).unwrap_or(false);
        Ok(QdbSample(Sample {
            ts,
            tag_id,
            value,
            quality,
            carried,
        }))
    }
}

struct TsCell(DateTime<Utc>);
struct U32Cell(u32);
struct F64Cell(f64);
struct BoolCell(bool);

impl<'de> Deserialize<'de> for TsCell {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        deserializer.deserialize_any(TsCellVisitor)
    }
}

struct TsCellVisitor;
impl<'de> Visitor<'de> for TsCellVisitor {
    type Value = TsCell;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("timestamp")
    }
    fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<TsCell, E> {
        parse_qdb_ts_str(v).map(TsCell).map_err(|_| E::custom(v))
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<TsCell, E> {
        DateTime::from_timestamp_millis(v)
            .map(TsCell)
            .ok_or_else(|| E::custom("ts millis"))
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<TsCell, E> {
        self.visit_i64(v as i64)
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<TsCell, E> {
        if v > 10_000_000_000_000.0 {
            let micros = v as i64;
            return DateTime::from_timestamp_micros(micros)
                .map(TsCell)
                .ok_or_else(|| E::custom("ts micros"));
        }
        if v > 10_000_000_000.0 {
            let millis = v as i64;
            return DateTime::from_timestamp_millis(millis)
                .map(TsCell)
                .ok_or_else(|| E::custom("ts millis"));
        }
        DateTime::from_timestamp(v as i64, 0)
            .map(TsCell)
            .ok_or_else(|| E::custom("ts seconds"))
    }
}

impl<'de> Deserialize<'de> for U32Cell {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        deserializer.deserialize_any(U32CellVisitor)
    }
}

struct U32CellVisitor;
impl<'de> Visitor<'de> for U32CellVisitor {
    type Value = U32Cell;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("u32")
    }
    fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<U32Cell, E> {
        v.parse().map(U32Cell).map_err(E::custom)
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<U32Cell, E> {
        Ok(U32Cell(v as u32))
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<U32Cell, E> {
        Ok(U32Cell(v as u32))
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<U32Cell, E> {
        Ok(U32Cell(v as u32))
    }
}

impl<'de> Deserialize<'de> for F64Cell {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        deserializer.deserialize_any(F64CellVisitor)
    }
}

struct F64CellVisitor;
impl<'de> Visitor<'de> for F64CellVisitor {
    type Value = F64Cell;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("f64")
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<F64Cell, E> {
        Ok(F64Cell(v))
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<F64Cell, E> {
        Ok(F64Cell(v as f64))
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<F64Cell, E> {
        Ok(F64Cell(v as f64))
    }
    fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<F64Cell, E> {
        v.parse().map(F64Cell).map_err(E::custom)
    }
}

impl<'de> Deserialize<'de> for BoolCell {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        deserializer.deserialize_any(BoolCellVisitor)
    }
}

struct BoolCellVisitor;
impl<'de> Visitor<'de> for BoolCellVisitor {
    type Value = BoolCell;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("bool")
    }
    fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<BoolCell, E> {
        Ok(BoolCell(v))
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<BoolCell, E> {
        Ok(BoolCell(v != 0))
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<BoolCell, E> {
        Ok(BoolCell(v != 0))
    }
    fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<BoolCell, E> {
        Ok(BoolCell(matches!(v, "true" | "t" | "1")))
    }
}

fn append_ilp_line(buf: &mut String, p: &Sample) {
    let _ = write!(buf, "samples tag_id={}i,value=", p.tag_id);
    append_ilp_float(buf, p.value);
    let _ = write!(
        buf,
        ",quality={}i {}\n",
        p.quality,
        p.ts.timestamp_nanos_opt().unwrap_or(0)
    );
}

#[async_trait]
impl Store for QuestDb {
    fn name(&self) -> &'static str {
        "questdb"
    }

    async fn ping(&self) -> Result<()> {
        self.exec(
            "CREATE TABLE IF NOT EXISTS samples (ts TIMESTAMP, tag_id SYMBOL CAPACITY 256 CACHE INDEX, value FLOAT, quality SHORT) timestamp(ts) PARTITION BY DAY WAL",
        )
        .await?;
        self.exec("CREATE TABLE IF NOT EXISTS tags (id INT, name SYMBOL, unit SYMBOL)")
            .await?;
        self.exec("SELECT 1").await?;
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
        self.send_ilp(body.as_bytes()).await
    }

    async fn locf(&self, tag_ids: &[u32], at: DateTime<Utc>) -> Result<Vec<Sample>> {
        let q = format!(
            "SELECT ts, tag_id, value, quality FROM samples WHERE tag_id IN ({}) AND ts <= '{}' LATEST ON ts PARTITION BY tag_id",
            join_symbol_ids(tag_ids),
            qdb_time(at)
        );
        self.exec_samples(&q).await
    }

    async fn range(&self, tag_ids: &[u32], from: DateTime<Utc>, to: DateTime<Utc>) -> Result<Vec<Sample>> {
        let ids = join_symbol_ids(tag_ids);
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
            "#,
            qdb_time(from),
            qdb_time(from),
            qdb_time(to)
        );
        self.exp_samples(&q).await
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
