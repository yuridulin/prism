using System.Buffers;
using System.Buffers.Text;
using System.Globalization;
using System.Net.Http.Headers;
using System.Text.Json;
using Prism.Api.Models;

namespace Prism.Api.Store;

public sealed class VictoriaMetricsStore : IStore
{
    private static readonly TimeSpan NearLookback = TimeSpan.FromHours(3);
    private static readonly TimeSpan FullLookback = TimeSpan.FromDays(400);

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

    public async Task<IReadOnlyList<Sample>> LocfAsync(IReadOnlyList<uint> tagIds, DateTimeOffset at, CancellationToken ct = default)
    {
        at = at.ToUniversalTime();
        var rows = await QueryLast(tagIds, at, NearLookback, ct);
        var missing = MissingTagIds(tagIds, rows);
        if (missing.Count == 0)
        {
            return rows;
        }

        var rest = await QueryLast(missing, at, FullLookback, ct);
        if (rest.Count == 0)
        {
            return rows;
        }

        var merged = new List<Sample>(rows.Count + rest.Count);
        merged.AddRange(rows);
        merged.AddRange(rest);
        return merged;
    }

    public async Task<IReadOnlyList<Sample>> RangeAsync(IReadOnlyList<uint> tagIds, DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default)
    {
        from = from.ToUniversalTime();
        to = to.ToUniversalTime();
        var head = await LocfAsync(tagIds, from, ct);
        foreach (var sample in head)
        {
            sample.Carried = true;
        }

        var tail = await ScanExport(tagIds, from, to, from, to, withMid: true, includeSeed: false, ct);
        return MergeRange(tagIds, head, tail);
    }

    public async Task<IReadOnlyList<Sample>> SampleAsync(
        IReadOnlyList<uint> tagIds,
        DateTimeOffset from,
        DateTimeOffset to,
        TimeSpan step,
        CancellationToken ct = default)
    {
        from = from.ToUniversalTime();
        to = to.ToUniversalTime();
        if (tagIds.Count == 0 || step <= TimeSpan.Zero)
        {
            return [];
        }

        var selector = SeriesSelector(tagIds);
        var look = VmLookbehind(FullLookback);
        var values = QueryRange(selector, "last_over_time", look, from, to, step, ct);
        var times = QueryRange(selector, "tlast_over_time", look, from, to, step, ct);
        await Task.WhenAll(values, times);
        return MergeSampleGrid(tagIds, await values, await times);
    }

    private async Task<IReadOnlyList<Sample>> QueryLast(
        IReadOnlyList<uint> tagIds,
        DateTimeOffset at,
        TimeSpan lookbehind,
        CancellationToken ct)
    {
        if (tagIds.Count == 0)
        {
            return [];
        }

        var selector = SeriesSelector(tagIds);
        var look = VmLookbehind(lookbehind);
        var unix = at.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture);
        var values = InstantQuery($"last_over_time({selector}[{look}])", unix, ct);
        var times = InstantQuery($"tlast_over_time({selector}[{look}])", unix, ct);
        await Task.WhenAll(values, times);
        return MergeLast(tagIds, await values, await times);
    }

    private async Task<List<VmPoint>> InstantQuery(string query, string unixTime, CancellationToken ct)
    {
        using var content = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["query"] = query,
            ["time"] = unixTime,
        });
        using var resp = await _http.PostAsync("api/v1/query", content, ct);
        await StoreUtil.EnsureSuccess(resp, "vm query", ct);
        await using var stream = await resp.Content.ReadAsStreamAsync(ct);
        using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: ct);
        return ParseInstant(doc);
    }

    private async Task<List<VmPoint>> QueryRange(
        string selector,
        string fn,
        string lookbehind,
        DateTimeOffset from,
        DateTimeOffset to,
        TimeSpan step,
        CancellationToken ct)
    {
        var query = $"{fn}({selector}[{lookbehind}])";
        var stepSec = Math.Max(1, (int)Math.Round(step.TotalSeconds)).ToString(CultureInfo.InvariantCulture);
        using var content = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["query"] = query,
            ["start"] = from.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture),
            ["end"] = to.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture),
            ["step"] = stepSec,
        });
        using var resp = await _http.PostAsync("api/v1/query_range", content, ct);
        await StoreUtil.EnsureSuccess(resp, "vm query_range", ct);
        await using var stream = await resp.Content.ReadAsStreamAsync(ct);
        using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: ct);
        return ParseRange(doc);
    }

    private async Task<IReadOnlyList<Sample>> ScanExport(
        IReadOnlyList<uint> tagIds,
        DateTimeOffset exportStart,
        DateTimeOffset exportEnd,
        DateTimeOffset from,
        DateTimeOffset to,
        bool withMid,
        bool includeSeed,
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
        if (includeSeed)
        {
            foreach (var id in tagIds)
            {
                if (best.TryGetValue(id, out var sample))
                {
                    seed.Add(sample);
                }
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

    private static string SeriesSelector(IReadOnlyList<uint> tagIds) =>
        $"prism_sample{{tag_id=~\"{JoinPipe(tagIds)}\"}}";

    private static string VmLookbehind(TimeSpan window)
    {
        if (window.TotalHours >= 24 && window.TotalHours % 24 == 0)
        {
            return ((int)window.TotalDays).ToString(CultureInfo.InvariantCulture) + "d";
        }

        if (window.TotalMinutes >= 60 && window.TotalMinutes % 60 == 0)
        {
            return ((int)window.TotalHours).ToString(CultureInfo.InvariantCulture) + "h";
        }

        return Math.Max(1, (int)Math.Round(window.TotalSeconds)).ToString(CultureInfo.InvariantCulture) + "s";
    }

    private readonly record struct VmPoint(uint TagId, ushort Quality, double Value, DateTimeOffset At);

    private static List<VmPoint> ParseInstant(JsonDocument doc)
    {
        var output = new List<VmPoint>();
        if (!doc.RootElement.TryGetProperty("data", out var data)
            || !data.TryGetProperty("result", out var result)
            || result.ValueKind != JsonValueKind.Array)
        {
            return output;
        }

        foreach (var row in result.EnumerateArray())
        {
            if (!TryMetric(row, out var id, out var quality))
            {
                continue;
            }

            if (!row.TryGetProperty("value", out var pair) || pair.GetArrayLength() < 2)
            {
                continue;
            }

            if (!TryNumber(pair[1], out var value))
            {
                continue;
            }

            output.Add(new VmPoint(id, quality, value, DateTimeOffset.UnixEpoch));
        }

        return output;
    }

    private static List<VmPoint> ParseRange(JsonDocument doc)
    {
        var output = new List<VmPoint>();
        if (!doc.RootElement.TryGetProperty("data", out var data)
            || !data.TryGetProperty("result", out var result)
            || result.ValueKind != JsonValueKind.Array)
        {
            return output;
        }

        foreach (var row in result.EnumerateArray())
        {
            if (!TryMetric(row, out var id, out var quality))
            {
                continue;
            }

            if (!row.TryGetProperty("values", out var values) || values.ValueKind != JsonValueKind.Array)
            {
                continue;
            }

            foreach (var pair in values.EnumerateArray())
            {
                if (pair.GetArrayLength() < 2 || !TryNumber(pair[0], out var unix) || !TryNumber(pair[1], out var value))
                {
                    continue;
                }

                output.Add(new VmPoint(id, quality, value, DateTimeOffset.FromUnixTimeSeconds((long)unix)));
            }
        }

        return output;
    }

    private static bool TryMetric(JsonElement row, out uint id, out ushort quality)
    {
        id = 0;
        quality = LocfQuality.Good;
        if (!row.TryGetProperty("metric", out var metric))
        {
            return false;
        }

        if (!metric.TryGetProperty("tag_id", out var tagEl)
            || !uint.TryParse(tagEl.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out id))
        {
            return false;
        }

        if (metric.TryGetProperty("quality", out var qEl)
            && ushort.TryParse(qEl.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var q))
        {
            quality = q;
        }

        return true;
    }

    private static bool TryNumber(JsonElement el, out double value)
    {
        value = 0;
        switch (el.ValueKind)
        {
            case JsonValueKind.Number:
                return el.TryGetDouble(out value);
            case JsonValueKind.String:
                return double.TryParse(el.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out value);
            default:
                return false;
        }
    }

    private static List<Sample> MergeLast(IReadOnlyList<uint> tagIds, List<VmPoint> values, List<VmPoint> times)
    {
        var observed = new Dictionary<uint, (DateTimeOffset Ts, ushort Quality)>(tagIds.Count);
        foreach (var point in times)
        {
            var ts = DateTimeOffset.FromUnixTimeSeconds((long)point.Value);
            if (!observed.TryGetValue(point.TagId, out var prev) || ts > prev.Ts)
            {
                observed[point.TagId] = (ts, point.Quality);
            }
        }

        var last = new Dictionary<uint, Sample>(tagIds.Count);
        foreach (var point in values)
        {
            if (!observed.TryGetValue(point.TagId, out var obs))
            {
                continue;
            }

            if (point.Quality != obs.Quality)
            {
                continue;
            }

            last[point.TagId] = new Sample
            {
                Ts = obs.Ts,
                TagId = point.TagId,
                Value = point.Value,
                Quality = point.Quality
            };
        }

        var output = new List<Sample>(tagIds.Count);
        foreach (var id in tagIds)
        {
            if (last.TryGetValue(id, out var sample))
            {
                output.Add(sample);
            }
        }

        return output;
    }

    private static List<Sample> MergeSampleGrid(IReadOnlyList<uint> tagIds, List<VmPoint> values, List<VmPoint> times)
    {
        var obsAt = new Dictionary<(uint Id, DateTimeOffset At), (DateTimeOffset Observed, ushort Quality)>();
        foreach (var point in times)
        {
            var observed = DateTimeOffset.FromUnixTimeSeconds((long)point.Value);
            obsAt[(point.TagId, point.At)] = (observed, point.Quality);
        }

        var output = new List<Sample>(values.Count);
        var order = new HashSet<uint>(tagIds);
        foreach (var point in values)
        {
            if (!order.Contains(point.TagId))
            {
                continue;
            }

            if (!obsAt.TryGetValue((point.TagId, point.At), out var obs) || point.Quality != obs.Quality)
            {
                output.Add(new Sample
                {
                    Ts = point.At,
                    TagId = point.TagId,
                    Value = point.Value,
                    Quality = LocfQuality.GoodLocf,
                    Carried = true
                });
                continue;
            }

            output.Add(new Sample
            {
                Ts = point.At,
                TagId = point.TagId,
                Value = point.Value,
                Quality = LocfQuality.Carry(point.Quality, obs.Observed, point.At),
                Carried = obs.Observed < point.At
            });
        }

        return output;
    }

    private static List<Sample> MergeRange(IReadOnlyList<uint> tagIds, IReadOnlyList<Sample> head, IReadOnlyList<Sample> tail)
    {
        var buckets = new Dictionary<uint, List<Sample>>(tagIds.Count);
        foreach (var id in tagIds)
        {
            buckets.TryAdd(id, []);
        }

        foreach (var sample in head)
        {
            sample.Carried = true;
            if (!buckets.TryGetValue(sample.TagId, out var bucket))
            {
                bucket = [];
                buckets[sample.TagId] = bucket;
            }

            bucket.Add(sample);
        }

        var extra = new List<Sample>();
        foreach (var sample in tail)
        {
            if (buckets.TryGetValue(sample.TagId, out var bucket))
            {
                bucket.Add(sample);
            }
            else
            {
                extra.Add(sample);
            }
        }

        var output = new List<Sample>(head.Count + tail.Count);
        foreach (var id in tagIds)
        {
            output.AddRange(buckets[id]);
        }

        output.AddRange(extra);
        return output;
    }

    private static List<uint> MissingTagIds(IReadOnlyList<uint> tagIds, IReadOnlyList<Sample> rows)
    {
        var found = new HashSet<uint>(rows.Select(s => s.TagId));
        var missing = new List<uint>();
        var seen = new HashSet<uint>();
        foreach (var id in tagIds)
        {
            if (!seen.Add(id) || found.Contains(id))
            {
                continue;
            }

            missing.Add(id);
        }

        return missing;
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
