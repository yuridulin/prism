using System.Diagnostics;
using Prism.Api.Metrics;
using Prism.Api.Models;

namespace Prism.Api.Store;

public sealed class ObservedStore(IStore inner) : IStore
{
    public string Name => inner.Name;

    public async Task PingAsync(CancellationToken ct = default)
    {
        var start = Stopwatch.StartNew();
        Exception? error = null;
        try
        {
            await inner.PingAsync(ct);
        }
        catch (Exception ex)
        {
            error = ex;
            throw;
        }
        finally
        {
            PrismMetrics.ObserveStorage(Name, "ping", start.Elapsed, error);
        }
    }

    public async Task WriteAsync(IReadOnlyList<Sample> samples, CancellationToken ct = default)
    {
        var start = Stopwatch.StartNew();
        Exception? error = null;
        try
        {
            await inner.WriteAsync(samples, ct);
        }
        catch (Exception ex)
        {
            error = ex;
            throw;
        }
        finally
        {
            PrismMetrics.ObserveStorage(Name, "write", start.Elapsed, error);
        }
    }

    public Task<IReadOnlyList<Sample>> LocfAsync(IReadOnlyList<uint> tagIds, DateTimeOffset at, CancellationToken ct = default) =>
        Observe("locf", () => inner.LocfAsync(tagIds, at, ct));

    public Task<IReadOnlyList<Sample>> RangeAsync(IReadOnlyList<uint> tagIds, DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default) =>
        Observe("range", () => inner.RangeAsync(tagIds, from, to, ct));

    public Task<IReadOnlyList<Sample>> SampleAsync(IReadOnlyList<uint> tagIds, DateTimeOffset from, DateTimeOffset to, TimeSpan step, CancellationToken ct = default) =>
        Observe("sample", () => inner.SampleAsync(tagIds, from, to, step, ct));

    public async Task UpsertTagsAsync(IReadOnlyList<Tag> tags, CancellationToken ct = default)
    {
        var start = Stopwatch.StartNew();
        Exception? error = null;
        try
        {
            await inner.UpsertTagsAsync(tags, ct);
        }
        catch (Exception ex)
        {
            error = ex;
            throw;
        }
        finally
        {
            PrismMetrics.ObserveStorage(Name, "tags", start.Elapsed, error);
        }
    }

    public Task<IReadOnlyList<Tag>> ListTagsAsync(CancellationToken ct = default) =>
        ObserveTags(() => inner.ListTagsAsync(ct));

    public ValueTask DisposeAsync() => inner.DisposeAsync();

    private async Task<IReadOnlyList<Sample>> Observe(string op, Func<Task<IReadOnlyList<Sample>>> action)
    {
        var start = Stopwatch.StartNew();
        Exception? error = null;
        try
        {
            return await action();
        }
        catch (Exception ex)
        {
            error = ex;
            throw;
        }
        finally
        {
            PrismMetrics.ObserveStorage(Name, op, start.Elapsed, error);
        }
    }

    private async Task<IReadOnlyList<Tag>> ObserveTags(Func<Task<IReadOnlyList<Tag>>> action)
    {
        var start = Stopwatch.StartNew();
        Exception? error = null;
        try
        {
            return await action();
        }
        catch (Exception ex)
        {
            error = ex;
            throw;
        }
        finally
        {
            PrismMetrics.ObserveStorage(Name, "tags", start.Elapsed, error);
        }
    }
}
