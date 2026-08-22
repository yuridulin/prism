using System.Buffers;
using System.Buffers.Text;
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
        _http = StoreUtil.CreatePooledHttp(TimeSpan.FromSeconds(30), _base + "/");
        _http.DefaultRequestHeaders.ExpectContinue = false;
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

        using var buf = new ByteWriter(64 * samples.Count);
        foreach (var sample in samples)
        {
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

        using var content = new ReadOnlyMemoryContent(buf.Written);
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
            $"match[]={Uri.EscapeDataString($"prism_sample{{tag_id=~\"{JoinPipe(tagIds)}\"}}")}" +
            $"&start={exportStart.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture)}" +
            $"&end={exportEnd.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture)}" +
            "&format=" + Uri.EscapeDataString("tag_id,quality,__value__,__timestamp__:unix_ms");
        using var resp = await _http.GetAsync(_base + "/api/v1/export/csv?" + qs, HttpCompletionOption.ResponseHeadersRead, ct);
        await StoreUtil.EnsureSuccess(resp, "vm export", ct);
        await using var stream = await resp.Content.ReadAsStreamAsync(ct);

        var best = new Dictionary<uint, Sample>(tagIds.Count);
        var mid = withMid ? new List<Sample>(65536) : null;
        var fromMs = from.ToUnixTimeMilliseconds();
        var toMs = to.ToUnixTimeMilliseconds();

        var rented = ArrayPool<byte>.Shared.Rent(64 << 10);
        var filled = 0;
        try
        {
            while (true)
            {
                var n = await stream.ReadAsync(rented.AsMemory(filled), ct);
                if (n == 0)
                {
                    break;
                }

                filled += n;
                var consumed = Consume(rented.AsSpan(0, filled), best, mid, fromMs, toMs, withMid);
                if (consumed > 0)
                {
                    filled -= consumed;
                    if (filled > 0)
                    {
                        Buffer.BlockCopy(rented, consumed, rented, 0, filled);
                    }
                }
                else if (filled == rented.Length)
                {
                    var bigger = ArrayPool<byte>.Shared.Rent(rented.Length * 2);
                    Buffer.BlockCopy(rented, 0, bigger, 0, filled);
                    ArrayPool<byte>.Shared.Return(rented);
                    rented = bigger;
                }
            }

            if (filled > 0)
            {
                ParseLine(rented.AsSpan(0, filled), best, mid, fromMs, toMs, withMid);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }

        var seed = new List<Sample>(tagIds.Count + (mid?.Count ?? 0));
        foreach (var id in tagIds)
        {
            if (best.TryGetValue(id, out var sample))
            {
                seed.Add(sample);
            }
        }

        if (mid is { Count: > 0 })
        {
            seed.AddRange(mid);
        }

        return seed;
    }

    private static string JoinPipe(IReadOnlyList<uint> ids)
    {
        var sb = new System.Text.StringBuilder(ids.Count * 6);
        for (var i = 0; i < ids.Count; i++)
        {
            if (i > 0)
            {
                sb.Append('|');
            }

            sb.Append(ids[i].ToString(CultureInfo.InvariantCulture));
        }

        return sb.ToString();
    }

    private static int Consume(
        ReadOnlySpan<byte> buf,
        Dictionary<uint, Sample> best,
        List<Sample>? mid,
        long fromMs,
        long toMs,
        bool withMid)
    {
        var start = 0;
        while (true)
        {
            var slice = buf[start..];
            var nl = slice.IndexOf((byte)'\n');
            if (nl < 0)
            {
                break;
            }

            ParseLine(slice[..nl], best, mid, fromMs, toMs, withMid);
            start += nl + 1;
        }

        return start;
    }

    private static void ParseLine(
        ReadOnlySpan<byte> line,
        Dictionary<uint, Sample> best,
        List<Sample>? mid,
        long fromMs,
        long toMs,
        bool withMid)
    {
        if (line.Length > 0 && line[^1] == (byte)'\r')
        {
            line = line[..^1];
        }

        if (line.Length == 0 || line[0] == (byte)'t')
        {
            return;
        }

        if (!TryParseExportLine(line, out var id, out var quality, out var val, out var ms))
        {
            return;
        }

        if (ms <= fromMs)
        {
            var t = DateTimeOffset.FromUnixTimeMilliseconds(ms);
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

            return;
        }

        if (withMid && ms <= toMs)
        {
            mid!.Add(new Sample
            {
                Ts = DateTimeOffset.FromUnixTimeMilliseconds(ms),
                TagId = id,
                Value = val,
                Quality = quality
            });
        }
    }

    private static bool TryParseExportLine(
        ReadOnlySpan<byte> line,
        out uint id,
        out ushort quality,
        out double val,
        out long ms)
    {
        id = 0;
        quality = 0;
        val = 0;
        ms = 0;
        var c1 = line.IndexOf((byte)',');
        if (c1 <= 0)
        {
            return false;
        }

        var rest = line[(c1 + 1)..];
        var c2 = rest.IndexOf((byte)',');
        if (c2 <= 0)
        {
            return false;
        }

        var rest2 = rest[(c2 + 1)..];
        var c3 = rest2.IndexOf((byte)',');
        if (c3 <= 0)
        {
            return false;
        }

        if (!Utf8Parser.TryParse(line[..c1], out id, out _))
        {
            return false;
        }

        if (Utf8Parser.TryParse(rest[..c2], out int q, out _))
        {
            quality = (ushort)q;
        }

        Utf8Parser.TryParse(rest2[..c3], out val, out _);
        return Utf8Parser.TryParse(rest2[(c3 + 1)..], out ms, out _);
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
