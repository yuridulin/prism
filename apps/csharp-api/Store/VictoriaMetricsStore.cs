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
        using var resp = await _http.GetAsync(_base + "/api/v1/export/csv?" + qs, HttpCompletionOption.ResponseHeadersRead, ct);
        await StoreUtil.EnsureSuccess(resp, "vm export", ct);
        await using var stream = await resp.Content.ReadAsStreamAsync(ct);
        using var doc = new StreamReader(stream);

        var best = new Dictionary<uint, Sample>(tagIds.Count);
        var mid = new List<Sample>(4096);
        while (await doc.ReadLineAsync(ct) is { } line)
        {
            if (line.Length == 0 || line.StartsWith("tag_id", StringComparison.Ordinal))
            {
                continue;
            }

            if (!TryParseExportLine(line, out var id, out var quality, out var val, out var t))
            {
                continue;
            }

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

    private static bool TryParseExportLine(string line, out uint id, out ushort quality, out double val, out DateTimeOffset t)
    {
        id = 0;
        quality = 0;
        val = 0;
        t = default;
        var span = line.AsSpan();
        var c1 = span.IndexOf(',');
        if (c1 <= 0)
        {
            return false;
        }

        var rest = span[(c1 + 1)..];
        var c2 = rest.IndexOf(',');
        if (c2 <= 0)
        {
            return false;
        }

        var rest2 = rest[(c2 + 1)..];
        var c3 = rest2.IndexOf(',');
        if (c3 <= 0)
        {
            return false;
        }

        if (!uint.TryParse(span[..c1], NumberStyles.Integer, CultureInfo.InvariantCulture, out id))
        {
            return false;
        }

        if (int.TryParse(rest[..c2], NumberStyles.Integer, CultureInfo.InvariantCulture, out var q))
        {
            quality = (ushort)q;
        }

        double.TryParse(rest2[..c3], NumberStyles.Float, CultureInfo.InvariantCulture, out val);
        if (!long.TryParse(rest2[(c3 + 1)..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var ms))
        {
            return false;
        }

        t = DateTimeOffset.FromUnixTimeMilliseconds(ms);
        return true;
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
