namespace Prism.Api;

public sealed record AppConfig(
    string HttpAddr,
    string Storage,
    string PostgresDsn,
    string ClickHouseUrl,
    string ClickHouseDb,
    string QuestDbUrl,
    string InfluxUrl,
    string InfluxToken,
    string InfluxOrg,
    string InfluxBucket,
    string VmUrl,
    string NatsUrl,
    string NatsSubject)
{
    public static readonly string[] Storages =
    [
        "timescaledb", "clickhouse", "questdb", "influxdb", "victoriametrics"
    ];

    public static AppConfig Load()
    {
        var storage = Env("PRISM_STORAGE", "questdb").ToLowerInvariant();
        if (!Storages.Contains(storage))
        {
            throw new InvalidOperationException($"unknown PRISM_STORAGE \"{storage}\"");
        }

        return new AppConfig(
            HttpAddr: Env("HTTP_ADDR", "0.0.0.0:8083"),
            Storage: storage,
            PostgresDsn: Env("POSTGRES_DSN", "postgres://prism:prism@timescaledb:5432/prism?sslmode=disable"),
            ClickHouseUrl: Env("CLICKHOUSE_URL", "http://prism:prism@clickhouse:8123"),
            ClickHouseDb: Env("CLICKHOUSE_DB", "prism"),
            QuestDbUrl: Env("QUESTDB_URL", "http://questdb:9000"),
            InfluxUrl: Env("INFLUX_URL", "http://influxdb:8086"),
            InfluxToken: Env("INFLUX_TOKEN", "prism-dev-token"),
            InfluxOrg: Env("INFLUX_ORG", "prism"),
            InfluxBucket: Env("INFLUX_BUCKET", "prism"),
            VmUrl: Env("VM_URL", "http://victoriametrics:8428"),
            NatsUrl: Env("NATS_URL", "nats://nats:4222"),
            NatsSubject: Env("NATS_SUBJECT", "prism.samples"));
    }

    public static string ToListenUrl(string addr)
    {
        if (addr.StartsWith("http://", StringComparison.OrdinalIgnoreCase) ||
            addr.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
        {
            return addr;
        }

        if (addr.StartsWith(':'))
        {
            return "http://0.0.0.0" + addr;
        }

        return "http://" + addr;
    }

    private static string Env(string key, string fallback)
    {
        var value = Environment.GetEnvironmentVariable(key);
        return string.IsNullOrEmpty(value) ? fallback : value;
    }
}
