mod metrics;
mod model;
mod read;
mod store;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::rejection::JsonRejection;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use chrono::Utc;
use metrics_exporter_prometheus::PrometheusHandle;
use model::{
    parse_write_payload, ErrorBody, ErrorDetail, Meta, TagList, TagWriteRequest, TagWriteResponse, ValuesRequest,
    WriteItem, WriteResponse, CONTRACT, OPS, STORAGES,
};
use store::Store;

#[derive(Clone)]
pub struct Config {
    pub http_addr: String,
    pub storage: String,
    pub postgres_dsn: String,
    pub clickhouse_url: String,
    pub clickhouse_db: String,
    pub questdb_url: String,
    pub questdb_ilp: String,
    pub influx_url: String,
    pub influx_token: String,
    pub influx_org: String,
    pub influx_bucket: String,
    pub vm_url: String,
    pub nats_url: String,
    pub nats_subject: String,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let storage = env_or("PRISM_STORAGE", "questdb").to_lowercase();
        if !STORAGES.contains(&storage.as_str()) {
            return Err(format!("unknown PRISM_STORAGE {storage:?}"));
        }
        Ok(Self {
            http_addr: listen_addr(&env_or("HTTP_ADDR", "0.0.0.0:8084")),
            storage,
            postgres_dsn: env_or(
                "POSTGRES_DSN",
                "postgres://prism:prism@timescaledb:5432/prism?sslmode=disable",
            ),
            clickhouse_url: env_or("CLICKHOUSE_URL", "http://prism:prism@clickhouse:8123"),
            clickhouse_db: env_or("CLICKHOUSE_DB", "prism"),
            questdb_url: env_or("QUESTDB_URL", "http://questdb:9000"),
            questdb_ilp: env_or("QUESTDB_ILP", "questdb:9009"),
            influx_url: env_or("INFLUX_URL", "http://influxdb:8086"),
            influx_token: env_or("INFLUX_TOKEN", "prism-dev-token"),
            influx_org: env_or("INFLUX_ORG", "prism"),
            influx_bucket: env_or("INFLUX_BUCKET", "prism"),
            vm_url: env_or("VM_URL", "http://victoriametrics:8428"),
            nats_url: env_or("NATS_URL", "nats://nats:4222"),
            nats_subject: env_or("NATS_SUBJECT", "prism.samples"),
        })
    }
}

fn env_or(key: &str, fallback: &str) -> String {
    std::env::var(key).ok().filter(|s| !s.is_empty()).unwrap_or_else(|| fallback.to_string())
}

fn listen_addr(raw: &str) -> String {
    if raw.starts_with(':') {
        format!("0.0.0.0{raw}")
    } else {
        raw.to_string()
    }
}

#[derive(Clone)]
struct AppState {
    store: Arc<dyn Store>,
    metrics: PrometheusHandle,
}

struct AppError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl AppError {
    fn invalid(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_request",
            message: msg.into(),
        }
    }

    fn storage(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "storage_error",
            message: msg.into(),
        }
    }

    fn unavailable(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "storage_unavailable",
            message: msg.into(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorBody {
                error: ErrorDetail {
                    code: self.code.to_string(),
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}

fn json_body<T>(body: Result<Json<T>, JsonRejection>) -> Result<T, AppError> {
    body.map(|Json(v)| v).map_err(|_| AppError::invalid("invalid json"))
}

async fn healthz() -> &'static str {
    "ok"
}

async fn readyz(State(state): State<AppState>) -> Result<&'static str, AppError> {
    state
        .store
        .ping()
        .await
        .map_err(|e| AppError::unavailable(e.to_string()))?;
    Ok("ready")
}

async fn metrics_endpoint(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(),
    )
}

async fn meta(State(state): State<AppState>) -> Json<Meta> {
    Json(Meta {
        backend: "rust".to_string(),
        storage: state.store.name().to_string(),
        storages: STORAGES.iter().map(|s| (*s).to_string()).collect(),
        contract: CONTRACT.to_string(),
        ops: OPS.iter().map(|s| (*s).to_string()).collect(),
    })
}

async fn write_samples(
    State(state): State<AppState>,
    body: Result<Json<Vec<WriteItem>>, JsonRejection>,
) -> Result<Json<WriteResponse>, AppError> {
    let items = json_body(body)?;
    if items.is_empty() {
        return Err(AppError::invalid("values array is required"));
    }
    let now = Utc::now();
    let samples: Vec<_> = items.into_iter().map(|s| s.normalize(now)).collect();
    let start = Instant::now();
    let err = state.store.write(&samples).await.err();
    crate::metrics::observe_backend(
        state.store.name(),
        "write",
        "http",
        samples.len(),
        start.elapsed(),
        err.is_some(),
    );
    if let Some(e) = err {
        return Err(AppError::storage(e.to_string()));
    }
    Ok(Json(WriteResponse {
        written: samples.len(),
    }))
}

async fn read_handler(
    State(state): State<AppState>,
    body: Result<Json<ValuesRequest>, JsonRejection>,
) -> Result<Json<model::ValuesResponse>, AppError> {
    serve_read(state, json_body(body)?).await
}

async fn serve_read(state: AppState, req: ValuesRequest) -> Result<Json<model::ValuesResponse>, AppError> {
    if req.tags_id.is_empty() {
        return Err(AppError::invalid("tagsId is required"));
    }
    let mode = req.mode();
    let start = Instant::now();
    let raw = match mode {
        "range" => {
            let from = req.old.ok_or_else(|| AppError::invalid("old and young are required"))?;
            let to = req.young.ok_or_else(|| AppError::invalid("old and young are required"))?;
            state.store.range(&req.tags_id, from, to).await
        }
        _ => state.store.locf(&req.tags_id, req.at()).await,
    };
    let err = raw.as_ref().err().map(|e| e.to_string());
    let items = raw.as_ref().map(|v| v.len()).unwrap_or(0);
    crate::metrics::observe_backend(
        state.store.name(),
        mode,
        "http",
        items,
        start.elapsed(),
        err.is_some(),
    );
    match raw {
        Ok(samples) => Ok(Json(read::assemble(&req, &samples))),
        Err(e) => Err(AppError::storage(e.to_string())),
    }
}

async fn list_tags(State(state): State<AppState>) -> Result<Json<TagList>, AppError> {
    let tags = state.store.list_tags().await.map_err(|e| AppError::storage(e.to_string()))?;
    Ok(Json(TagList { tags }))
}

async fn upsert_tags(
    State(state): State<AppState>,
    body: Result<Json<TagWriteRequest>, JsonRejection>,
) -> Result<Json<TagWriteResponse>, AppError> {
    let req = json_body(body)?;
    if req.tags.is_empty() {
        return Err(AppError::invalid("tags is required"));
    }
    state
        .store
        .upsert_tags(&req.tags)
        .await
        .map_err(|e| AppError::storage(e.to_string()))?;
    Ok(Json(TagWriteResponse {
        upserted: req.tags.len(),
    }))
}

async fn instrument(State(state): State<AppState>, req: Request<axum::body::Body>, next: Next) -> Response {
    let path = req.uri().path().to_string();
    let method = req.method().to_string();
    if matches!(path.as_str(), "/metrics" | "/healthz" | "/readyz") {
        return next.run(req).await;
    }
    let start = Instant::now();
    let res = next.run(req).await;
    let status = res.status().as_u16().to_string();
    let route = path.trim_start_matches('/').replace('/', "_");
    crate::metrics::observe_api(state.store.name(), &route, &method, &status, start.elapsed());
    res
}

fn app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_endpoint))
        .route("/api/meta", get(meta))
        .route("/v1/meta", get(meta))
        .route("/api/tags", get(list_tags).post(upsert_tags))
        .route("/api/values", post(read_handler).put(write_samples))
        .layer(middleware::from_fn_with_state(state.clone(), instrument))
        .with_state(state)
}

async fn subscribe_nats(url: String, subject: String, store: Arc<dyn Store>) {
    let client = match async_nats::connect(&url).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("nats unavailable, HTTP-only mode: {e}");
            return;
        }
    };
    let mut sub = match client.queue_subscribe(subject.clone(), "rust".to_string()).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("nats unavailable, HTTP-only mode: {e}");
            return;
        }
    };
    tracing::info!("nats subscribed subject={subject} queue=rust");
    use futures::StreamExt;
    while let Some(msg) = sub.next().await {
        if let Err(e) = handle_nats(&store, &msg.payload).await {
            tracing::warn!("nats write error: {e}");
        }
    }
}

async fn handle_nats(store: &Arc<dyn Store>, payload: &[u8]) -> Result<(), String> {
    let items = parse_write_payload(payload)?;
    if items.is_empty() {
        return Ok(());
    }
    let now = Utc::now();
    let samples: Vec<_> = items.into_iter().map(|s| s.normalize(now)).collect();
    let start = Instant::now();
    let err = store.write(&samples).await.err();
    crate::metrics::observe_backend(store.name(), "write", "nats", samples.len(), start.elapsed(), err.is_some());
    if let Some(e) = err {
        return Err(e.to_string());
    }
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {},
            _ = term.recv() => {},
        }
    }
    #[cfg(not(unix))]
    ctrl_c.await;
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    let cfg = Config::from_env().unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(1);
    });

    let handle = crate::metrics::install();
    let store = store::open(&cfg.storage, &cfg).await.unwrap_or_else(|e| {
        eprintln!("store: {e}");
        std::process::exit(1);
    });
    let store: Arc<dyn Store> = Arc::new(store);

    tokio::spawn(subscribe_nats(cfg.nats_url.clone(), cfg.nats_subject.clone(), store.clone()));

    let state = AppState {
        store: store.clone(),
        metrics: handle,
    };
    let addr: SocketAddr = cfg.http_addr.parse().unwrap_or_else(|_| {
        eprintln!("invalid HTTP_ADDR {}", cfg.http_addr);
        std::process::exit(1);
    });
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap_or_else(|e| {
        eprintln!("bind {addr}: {e}");
        std::process::exit(1);
    });
    tracing::info!("rust-api listening on {addr} storage={}", cfg.storage);
    axum::serve(listener, app(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server");
}
