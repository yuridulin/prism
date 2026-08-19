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
            "questdb" => new QuestDbStore(cfg.QuestDbUrl, cfg.QuestDbIlp),
            "influxdb" => new InfluxStore(cfg),
            "victoriametrics" => new VictoriaMetricsStore(cfg.VmUrl),
            _ => throw new InvalidOperationException($"unsupported storage \"{cfg.Storage}\"")
        });
}

internal sealed class FailedStore(string name, string reason) : IStore
{
    public string Name => name;

    public Task PingAsync(CancellationToken ct = default) => Fail();

    public Task WriteAsync(IReadOnlyList<Sample> samples, CancellationToken ct = default) => Fail();

    public Task<IReadOnlyList<Sample>> LocfAsync(IReadOnlyList<uint> tagIds, DateTimeOffset at, CancellationToken ct = default) =>
        Fail<IReadOnlyList<Sample>>();

    public Task<IReadOnlyList<Sample>> RangeAsync(IReadOnlyList<uint> tagIds, DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default) =>
        Fail<IReadOnlyList<Sample>>();

    public Task UpsertTagsAsync(IReadOnlyList<Tag> tags, CancellationToken ct = default) => Fail();

    public Task<IReadOnlyList<Tag>> ListTagsAsync(CancellationToken ct = default) => Fail<IReadOnlyList<Tag>>();

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private Task Fail() => Task.FromException(new InvalidOperationException(reason));

    private Task<T> Fail<T>() => Task.FromException<T>(new InvalidOperationException(reason));
}
