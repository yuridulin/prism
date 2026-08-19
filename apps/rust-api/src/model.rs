use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const CONTRACT: &str = "v1.1";
pub const QUALITY_GOOD: u16 = 192;
pub const OPS: &[&str] = &["write", "locf", "range", "sample", "twavg", "tags"];
pub const STORAGES: &[&str] = &[
    "timescaledb",
    "clickhouse",
    "questdb",
    "influxdb",
    "victoriametrics",
];

fn is_false(v: &bool) -> bool {
    !*v
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    pub ts: DateTime<Utc>,
    pub tag_id: u32,
    pub value: f64,
    pub quality: u16,
    #[serde(default, skip_serializing_if = "is_false")]
    pub carried: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WriteSample {
    #[serde(default)]
    pub ts: Option<DateTime<Utc>>,
    pub tag_id: u32,
    pub value: f64,
    #[serde(default)]
    pub quality: Option<u16>,
}

impl WriteSample {
    pub fn normalize(self, now: DateTime<Utc>) -> Sample {
        Sample {
            ts: self.ts.unwrap_or(now),
            tag_id: self.tag_id,
            value: self.value,
            quality: self.quality.unwrap_or(QUALITY_GOOD),
            carried: false,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct WriteRequest {
    pub samples: Vec<WriteSample>,
}

#[derive(Debug, Serialize)]
pub struct WriteResponse {
    pub written: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    pub id: u32,
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub unit: String,
}

#[derive(Debug, Serialize)]
pub struct TagList {
    pub tags: Vec<Tag>,
}

#[derive(Debug, Deserialize)]
pub struct TagWriteRequest {
    pub tags: Vec<Tag>,
}

#[derive(Debug, Serialize)]
pub struct TagWriteResponse {
    pub upserted: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReadRequest {
    #[serde(default)]
    pub mode: String,
    pub tag_ids: Vec<u32>,
    #[serde(default)]
    pub at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub from: Option<DateTime<Utc>>,
    #[serde(default)]
    pub to: Option<DateTime<Utc>>,
    #[serde(default)]
    pub step: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Series {
    pub tag_id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<f64>,
    pub samples: Vec<Sample>,
}

#[derive(Debug, Serialize)]
pub struct ReadResult {
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub step: String,
    pub series: Vec<Series>,
}

#[derive(Debug, Serialize)]
pub struct Meta {
    pub backend: String,
    pub storage: String,
    pub storages: Vec<String>,
    pub contract: String,
    pub ops: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub error: ErrorDetail,
}

#[derive(Debug, Serialize)]
pub struct ErrorDetail {
    pub code: String,
    pub message: String,
}

pub fn valid_mode(mode: &str) -> bool {
    matches!(mode, "locf" | "range" | "sample" | "twavg")
}
