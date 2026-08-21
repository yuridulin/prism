use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const CONTRACT: &str = "v1.2";
pub const QUALITY_GOOD: u16 = 192;
pub const OPS: &[&str] = &["write", "locf", "range", "tags"];
pub const STORAGES: &[&str] = &[
    "timescaledb",
    "clickhouse",
    "questdb",
    "influxdb",
    "victoriametrics",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    pub ts: DateTime<Utc>,
    pub tag_id: u32,
    pub value: f64,
    pub quality: u16,
    #[serde(default)]
    pub carried: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteItem {
    #[serde(default, alias = "tag_id")]
    pub id: Option<u32>,
    #[serde(default, alias = "ts")]
    pub date: Option<DateTime<Utc>>,
    pub value: f64,
    #[serde(default)]
    pub quality: Option<u16>,
}

impl WriteItem {
    pub fn normalize(self, now: DateTime<Utc>) -> Sample {
        Sample {
            ts: self.date.unwrap_or(now),
            tag_id: self.id.unwrap_or(0),
            value: self.value,
            quality: self.quality.unwrap_or(QUALITY_GOOD),
            carried: false,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct SamplesWrap {
    #[serde(default)]
    pub samples: Vec<WriteItem>,
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
#[serde(rename_all = "camelCase")]
pub struct ValuesRequest {
    #[serde(default)]
    pub request_key: String,
    #[serde(default)]
    pub tags_id: Vec<u32>,
    #[serde(default)]
    pub exact: Option<DateTime<Utc>>,
    #[serde(default)]
    pub old: Option<DateTime<Utc>>,
    #[serde(default)]
    pub young: Option<DateTime<Utc>>,
}

impl ValuesRequest {
    pub fn mode(&self) -> &'static str {
        if self.old.is_some() && self.young.is_some() {
            "range"
        } else {
            "locf"
        }
    }

    pub fn at(&self) -> DateTime<Utc> {
        self.exact.unwrap_or_else(Utc::now)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ValueRecord {
    pub date: DateTime<Utc>,
    pub value: f64,
    pub quality: u16,
}

#[derive(Debug, Clone, Serialize)]
pub struct ValuesTag {
    pub id: u32,
    pub values: Vec<ValueRecord>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ValuesResponse {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub request_key: String,
    pub tags: Vec<ValuesTag>,
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

pub fn parse_write_payload(payload: &[u8]) -> Result<Vec<WriteItem>, String> {
    let trimmed = payload.iter().position(|&b| !b.is_ascii_whitespace()).unwrap_or(0);
    let body = &payload[trimmed..];
    if body.first() == Some(&b'[') {
        return serde_json::from_slice(body).map_err(|e| e.to_string());
    }
    if let Ok(wrap) = serde_json::from_slice::<SamplesWrap>(body) {
        if !wrap.samples.is_empty() {
            return Ok(wrap.samples);
        }
    }
    serde_json::from_slice::<WriteItem>(body)
        .map(|one| vec![one])
        .map_err(|e| e.to_string())
}
