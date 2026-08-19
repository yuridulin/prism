using Prism.Api.Models;

namespace Prism.Api.Store;

public interface IStore : IAsyncDisposable
{
    string Name { get; }
    Task PingAsync(CancellationToken ct = default);
    Task WriteAsync(IReadOnlyList<Sample> samples, CancellationToken ct = default);
    Task<IReadOnlyList<Sample>> LocfAsync(IReadOnlyList<uint> tagIds, DateTimeOffset at, CancellationToken ct = default);
    Task<IReadOnlyList<Sample>> RangeAsync(IReadOnlyList<uint> tagIds, DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default);
    Task UpsertTagsAsync(IReadOnlyList<Tag> tags, CancellationToken ct = default);
    Task<IReadOnlyList<Tag>> ListTagsAsync(CancellationToken ct = default);
}

public static class StoreFactory
{
    public static IStore Create(AppConfig cfg) =>
        new ObservedStore(cfg.Storage switch
        {
            "timescaledb" => new TimescaleStore(cfg.PostgresDsn),
            "clickhouse" => new ClickHouseStore(cfg.ClickHouseUrl, cfg.ClickHouseDb),
            "questdb" => new QuestDbStore(cfg.QuestDbUrl),
            "influxdb" => new InfluxStore(cfg),
            "victoriametrics" => new VictoriaMetricsStore(cfg.VmUrl),
            _ => throw new InvalidOperationException($"unsupported storage \"{cfg.Storage}\"")
        });
}
