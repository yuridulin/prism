namespace Prism.Api;

public sealed record AppConfig(
    string HttpAddr,
    string Storage,
    string PostgresDsn,
    string ClickHouseUrl,
    string ClickHouseDb,
    string QuestDbUrl,
    string QuestDbIlp,
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
            QuestDbIlp: Env("QUESTDB_ILP", "questdb:9009"),
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

    public static string ToNpgsql(string dsn)
    {
        if (!dsn.Contains("://", StringComparison.Ordinal))
        {
            return dsn;
        }

        var uri = new Uri(dsn.Replace("postgres://", "postgresql://", StringComparison.OrdinalIgnoreCase));
        var userInfo = uri.UserInfo.Split(':', 2);
        var user = Uri.UnescapeDataString(userInfo[0]);
        var password = userInfo.Length > 1 ? Uri.UnescapeDataString(userInfo[1]) : "";
        var ssl = "Disable";
        foreach (var part in uri.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var kv = part.Split('=', 2);
            if (kv.Length != 2 || !kv[0].Equals("sslmode", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            ssl = kv[1].ToLowerInvariant() switch
            {
                "require" => "Require",
                "prefer" => "Prefer",
                _ => "Disable"
            };
        }

        var port = uri.Port > 0 ? uri.Port : 5432;
        var database = uri.AbsolutePath.Trim('/');
        return $"Host={uri.Host};Port={port};Username={user};Password={password};Database={database};SSL Mode={ssl}";
    }

    private static string Env(string key, string fallback)
    {
        var value = Environment.GetEnvironmentVariable(key);
        return string.IsNullOrEmpty(value) ? fallback : value;
    }
}
