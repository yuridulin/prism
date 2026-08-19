use std::time::Duration;

use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

pub const BACKEND: &str = "rust";

pub fn install() -> PrometheusHandle {
    PrometheusBuilder::new()
        .set_buckets(&[
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ])
        .expect("prometheus buckets")
        .install_recorder()
        .expect("prometheus recorder")
}

fn result_label(err: bool) -> &'static str {
    if err {
        "error"
    } else {
        "ok"
    }
}

pub fn observe_api(storage: &str, route: &str, method: &str, status: &str, d: Duration) {
    let labels = [
        ("backend", BACKEND.to_string()),
        ("storage", storage.to_string()),
        ("route", route.to_string()),
        ("method", method.to_string()),
        ("status", status.to_string()),
    ];
    counter!("prism_api_requests_total", &labels).increment(1);
    histogram!(
        "prism_api_request_duration_seconds",
        "backend" => BACKEND.to_string(),
        "storage" => storage.to_string(),
        "route" => route.to_string(),
        "method" => method.to_string()
    )
    .record(d.as_secs_f64());
}

pub fn observe_backend(storage: &str, op: &str, source: &str, items: usize, d: Duration, err: bool) {
    counter!(
        "prism_backend_ops_total",
        "backend" => BACKEND.to_string(),
        "storage" => storage.to_string(),
        "op" => op.to_string(),
        "source" => source.to_string(),
        "result" => result_label(err).to_string()
    )
    .increment(1);
    histogram!(
        "prism_backend_op_duration_seconds",
        "backend" => BACKEND.to_string(),
        "storage" => storage.to_string(),
        "op" => op.to_string(),
        "source" => source.to_string()
    )
    .record(d.as_secs_f64());
    if !err && items > 0 {
        counter!(
            "prism_backend_items_total",
            "backend" => BACKEND.to_string(),
            "storage" => storage.to_string(),
            "op" => op.to_string(),
            "source" => source.to_string()
        )
        .increment(items as u64);
    }
}

pub fn observe_storage(storage: &str, op: &str, d: Duration, err: bool) {
    counter!(
        "prism_storage_ops_total",
        "backend" => BACKEND.to_string(),
        "storage" => storage.to_string(),
        "op" => op.to_string(),
        "result" => result_label(err).to_string()
    )
    .increment(1);
    histogram!(
        "prism_storage_op_duration_seconds",
        "backend" => BACKEND.to_string(),
        "storage" => storage.to_string(),
        "op" => op.to_string()
    )
    .record(d.as_secs_f64());
    if op == "ping" {
        gauge!(
            "prism_storage_up",
            "backend" => BACKEND.to_string(),
            "storage" => storage.to_string()
        )
        .set(if err { 0.0 } else { 1.0 });
    }
}
