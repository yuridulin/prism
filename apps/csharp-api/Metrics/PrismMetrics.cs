using Prometheus;

namespace Prism.Api.Metrics;

public static class PrismMetrics
{
    public const string Backend = "csharp";

    private static readonly double[] DefBuckets = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];

    private static readonly Counter ApiRequests = Prometheus.Metrics.CreateCounter(
        "prism_api_requests_total",
        "HTTP requests by route",
        new CounterConfiguration { LabelNames = ["backend", "storage", "route", "method", "status"] });

    private static readonly Histogram ApiDuration = Prometheus.Metrics.CreateHistogram(
        "prism_api_request_duration_seconds",
        "HTTP request latency",
        new HistogramConfiguration { LabelNames = ["backend", "storage", "route", "method"], Buckets = DefBuckets });

    private static readonly Counter BackendOps = Prometheus.Metrics.CreateCounter(
        "prism_backend_ops_total",
        "Application operations",
        new CounterConfiguration { LabelNames = ["backend", "storage", "op", "source", "result"] });

    private static readonly Histogram BackendDuration = Prometheus.Metrics.CreateHistogram(
        "prism_backend_op_duration_seconds",
        "Application operation latency",
        new HistogramConfiguration { LabelNames = ["backend", "storage", "op", "source"], Buckets = DefBuckets });

    private static readonly Counter BackendItems = Prometheus.Metrics.CreateCounter(
        "prism_backend_items_total",
        "Points written or samples returned",
        new CounterConfiguration { LabelNames = ["backend", "storage", "op", "source"] });

    private static readonly Counter StorageOps = Prometheus.Metrics.CreateCounter(
        "prism_storage_ops_total",
        "Storage adapter operations",
        new CounterConfiguration { LabelNames = ["backend", "storage", "op", "result"] });

    private static readonly Histogram StorageDuration = Prometheus.Metrics.CreateHistogram(
        "prism_storage_op_duration_seconds",
        "Storage adapter latency",
        new HistogramConfiguration { LabelNames = ["backend", "storage", "op"], Buckets = DefBuckets });

    private static readonly Gauge StorageUp = Prometheus.Metrics.CreateGauge(
        "prism_storage_up",
        "1 if the last storage ping succeeded",
        new GaugeConfiguration { LabelNames = ["backend", "storage"] });

    public static void ObserveApi(string storage, string route, string method, int status, TimeSpan duration)
    {
        ApiRequests.WithLabels(Backend, storage, route, method, status.ToString()).Inc();
        ApiDuration.WithLabels(Backend, storage, route, method).Observe(duration.TotalSeconds);
    }

    public static void ObserveBackend(string storage, string op, string source, int items, TimeSpan duration, Exception? error)
    {
        BackendOps.WithLabels(Backend, storage, op, source, Result(error)).Inc();
        BackendDuration.WithLabels(Backend, storage, op, source).Observe(duration.TotalSeconds);
        if (error is null && items > 0)
        {
            BackendItems.WithLabels(Backend, storage, op, source).Inc(items);
        }
    }

    public static void ObserveStorage(string storage, string op, TimeSpan duration, Exception? error)
    {
        StorageOps.WithLabels(Backend, storage, op, Result(error)).Inc();
        StorageDuration.WithLabels(Backend, storage, op).Observe(duration.TotalSeconds);
        if (op == "ping")
        {
            StorageUp.WithLabels(Backend, storage).Set(error is null ? 1 : 0);
        }
    }

    private static string Result(Exception? error)
    {
        if (error is null)
        {
            return "ok";
        }

        return error is KeyNotFoundException ? "not_found" : "error";
    }
}
