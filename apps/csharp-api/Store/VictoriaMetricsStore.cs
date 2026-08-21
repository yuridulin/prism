using System.Globalization;
using System.Net.Http.Headers;
using Prism.Api.Models;

namespace Prism.Api.Store;

public sealed class VictoriaMetricsStore : IStore
{
    private static readonly TimeSpan Lookback = TimeSpan.FromHours(2); // archive max gap is 1h

    private readonly string _base;
    private readonly HttpClient _http;
    private readonly CatalogMem _tags = new();

    public VictoriaMetricsStore(string url)
    {
        _base = url.TrimEnd('/');
        _http = StoreUtil.CreatePooledHttp(TimeSpan.FromSeconds(15), _base + "/");
    }

    public string Name => "victoriametrics";

    public async Task PingAsync(CancellationToken ct = default)
    {
        using var resp = await _http.GetAsync(_base + "/health", ct);
        if ((int)resp.StatusCode >= 300)
        {
            throw new InvalidOperationException($"vm health status {(int)resp.StatusCode}");
        }
    }

    public async Task WriteAsync(IReadOnlyList<Sample> samples, CancellationToken ct = default)
    {
        if (samples.Count == 0)
        {
            return;
        }

        using var buf = new ByteWriter(80 * samples.Count);
        foreach (var sample in samples)
        {
            // Same ILP as Go: empty measurement, quality label, field prism_sample.
            buf.AppendAscii(",tag_id=");
            buf.AppendUInt(sample.TagId);
            buf.AppendAscii(",quality=");
            buf.AppendUShort(sample.Quality);
            buf.AppendAscii(" prism_sample=");
            buf.AppendIlpFloat(sample.Value);
            buf.AppendByte((byte)' ');
            buf.AppendLong(StoreUtil.UnixNano(sample.Ts));
            buf.AppendByte((byte)'\n');
        }

        using var content = new ByteArrayContent(buf.Buffer, 0, buf.Length);
        content.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
        using var resp = await _http.PostAsync("write?precision=ns", content, ct);
        await StoreUtil.EnsureSuccess(resp, "vm write", ct);
    }

    public Task<IReadOnlyList<Sample>> LocfAsync(IReadOnlyList<uint> tagIds, DateTimeOffset at, CancellationToken ct = default)
    {
        at = at.ToUniversalTime();
        return ScanExport(tagIds, at - Lookback, at, at, at, withMid: false, ct);
    }

    public Task<IReadOnlyList<Sample>> RangeAsync(IReadOnlyList<uint> tagIds, DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default)
    {
        from = from.ToUniversalTime();
        to = to.ToUniversalTime();
        return ScanExport(tagIds, from - Lookback, to, from, to, withMid: true, ct);
    }

    private async Task<IReadOnlyList<Sample>> ScanExport(
        IReadOnlyList<uint> tagIds,
        DateTimeOffset exportStart,
        DateTimeOffset exportEnd,
        DateTimeOffset from,
        DateTimeOffset to,
        bool withMid,
        CancellationToken ct)
    {
        if (tagIds.Count == 0)
        {
            return [];
        }

        var qs =
            $"match[]={Uri.EscapeDataString($"prism_sample{{tag_id=~\"{string.Join('|', tagIds)}\"}}")}" +
            $"&start={exportStart.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture)}" +
            $"&end={exportEnd.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture)}" +
            "&format=" + Uri.EscapeDataString("tag_id,quality,__value__,__timestamp__:unix_ms");
        using var resp = await _http.GetAsync(_base + "/api/v1/export/csv?" + qs, ct);
        await StoreUtil.EnsureSuccess(resp, "vm export", ct);
        await using var stream = await resp.Content.ReadAsStreamAsync(ct);
        using var doc = new StreamReader(stream);

        var best = new Dictionary<uint, Sample>(tagIds.Count);
        var mid = new List<Sample>();
        while (await doc.ReadLineAsync(ct) is { } line)
        {
            if (string.IsNullOrWhiteSpace(line) || line.StartsWith("tag_id", StringComparison.Ordinal))
            {
                continue;
            }

            var parts = line.Split(',');
            if (parts.Length < 4 || !uint.TryParse(parts[0], out var id))
            {
                continue;
            }

            ushort quality = 0;
            if (int.TryParse(parts[1], out var q))
            {
                quality = (ushort)q;
            }

            double.TryParse(parts[2], NumberStyles.Float, CultureInfo.InvariantCulture, out var val);
            if (!long.TryParse(parts[3], NumberStyles.Integer, CultureInfo.InvariantCulture, out var ms))
            {
                continue;
            }

            var t = DateTimeOffset.FromUnixTimeMilliseconds(ms);
            if (t <= from)
            {
                if (!best.TryGetValue(id, out var prev) || t > prev.Ts)
                {
                    best[id] = new Sample
                    {
                        Ts = t,
                        TagId = id,
                        Value = val,
                        Quality = quality,
                        Carried = withMid
                    };
                }
                continue;
            }

            if (withMid && t <= to)
            {
                mid.Add(new Sample
                {
                    Ts = t,
                    TagId = id,
                    Value = val,
                    Quality = quality
                });
            }
        }

        var seed = new List<Sample>(tagIds.Count);
        foreach (var id in tagIds)
        {
            if (best.TryGetValue(id, out var sample))
            {
                seed.Add(sample);
            }
        }

        if (!withMid)
        {
            return seed;
        }

        seed.AddRange(mid);
        return seed;
    }

    public Task UpsertTagsAsync(IReadOnlyList<Tag> tags, CancellationToken ct = default)
    {
        _tags.Upsert(tags);
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<Tag>> ListTagsAsync(CancellationToken ct = default) =>
        Task.FromResult(_tags.List());

    public ValueTask DisposeAsync()
    {
        _http.Dispose();
        return ValueTask.CompletedTask;
    }
}
