from prometheus_client import Counter, Histogram

INGEST_POINTS = Counter(
    "prism_ingest_points_total",
    "Written time-series points",
    ["backend", "storage"],
)
INGEST_ERRORS = Counter(
    "prism_ingest_errors_total",
    "Failed ingest batches",
    ["backend", "storage"],
)
QUERY_DURATION = Histogram(
    "prism_query_duration_seconds",
    "Query latency",
    ["backend", "storage"],
)
QUERY_ERRORS = Counter(
    "prism_query_errors_total",
    "Failed queries",
    ["backend", "storage"],
)
