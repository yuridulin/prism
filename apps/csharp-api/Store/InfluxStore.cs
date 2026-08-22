using System.Globalization;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using Prism.Api.Models;

namespace Prism.Api.Store;

public sealed class InfluxStore : IStore
{
    private readonly HttpClient _http;
    private readonly string _org;
    private readonly string _bucket;
    private readonly string _writePath;
    private readonly CatalogMem _tags = new();

    public InfluxStore(AppConfig cfg)
    {
        _org = cfg.InfluxOrg;
        _bucket = cfg.InfluxBucket;
        _writePath = "api/v2/write?org=" + Uri.EscapeDataString(_org)
                     + "&bucket=" + Uri.EscapeDataString(_bucket)
                     + "&precision=ns";
        _http = StoreUtil.CreatePooledHttp(TimeSpan.FromSeconds(30), cfg.InfluxUrl.TrimEnd('/') + "/");
        _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Token", cfg.InfluxToken);
    }

    public string Name => "influxdb";

    public async Task PingAsync(CancellationToken ct = default)
    {
        using var resp = await _http.GetAsync("health", ct);
        if (!resp.IsSuccessStatusCode)
        {
            throw new InvalidOperationException("influxdb ping failed");
        }

        await EnsureDbrp(ct);
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
            buf.AppendAscii("samples,tag_id=");
            buf.AppendUInt(sample.TagId);
            buf.AppendAscii(" value=");
            buf.AppendIlpFloat(sample.Value);
            buf.AppendAscii(",quality=");
            buf.AppendUShort(sample.Quality);
            buf.AppendAscii("i ");
            buf.AppendLong(StoreUtil.UnixNano(sample.Ts));
            buf.AppendByte((byte)'\n');
        }

        using var content = new ByteArrayContent(buf.Buffer, 0, buf.Length);
        content.Headers.ContentType = new MediaTypeHeaderValue("text/plain") { CharSet = "utf-8" };
        using var resp = await _http.PostAsync(_writePath, content, ct);
        await StoreUtil.EnsureSuccess(resp, "influxdb write", ct);
    }

    public Task<IReadOnlyList<Sample>> LocfAsync(IReadOnlyList<uint> tagIds, DateTimeOffset at, CancellationToken ct = default) =>
        QueryLast(tagIds, at.ToUniversalTime(), carried: false, ct);

    public async Task<IReadOnlyList<Sample>> RangeAsync(IReadOnlyList<uint> tagIds, DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default)
    {
        var seed = await QueryLast(tagIds, from.ToUniversalTime(), carried: true, ct);
        var mid = await QueryWindow(tagIds, from.ToUniversalTime(), to.ToUniversalTime(), ct);
        return seed.Concat(mid).ToList();
    }

    public async Task<IReadOnlyList<Sample>> SampleAsync(
        IReadOnlyList<uint> tagIds,
        DateTimeOffset from,
        DateTimeOffset to,
        TimeSpan step,
        CancellationToken ct = default)
    {
        var raw = await RangeAsync(tagIds, from, to, ct);
        return SampleStretch.Fill(tagIds, raw, from, to, step);
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

    private async Task<IReadOnlyList<Sample>> QueryLast(IReadOnlyList<uint> tagIds, DateTimeOffset stop, bool carried, CancellationToken ct)
    {
        var q = $"SELECT last(\"value\") AS \"value\", last(\"quality\") AS \"quality\" FROM \"samples\" WHERE time <= {QlTime(stop)} AND {TagRe(tagIds)} GROUP BY \"tag_id\"";
        return await QueryInfluxQl(q, carried, ct);
    }

    private async Task<IReadOnlyList<Sample>> QueryWindow(IReadOnlyList<uint> tagIds, DateTimeOffset from, DateTimeOffset to, CancellationToken ct)
    {
        var q = $"SELECT \"value\", \"quality\" FROM \"samples\" WHERE time > {QlTime(from)} AND time <= {QlTime(to)} AND {TagRe(tagIds)}";
        return await QueryInfluxQl(q, carried: false, ct);
    }

    private async Task<IReadOnlyList<Sample>> QueryInfluxQl(string q, bool carried, CancellationToken ct)
    {
        using var content = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["org"] = _org,
            ["bucket"] = _bucket,
            ["db"] = _bucket,
            ["epoch"] = "ms",
            ["q"] = q
        });
        using var req = new HttpRequestMessage(HttpMethod.Post, "query") { Content = content };
        req.Headers.Accept.Clear();
        req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/csv"));
        using var resp = await _http.SendAsync(req, ct);
        await StoreUtil.EnsureSuccess(resp, "influxdb query", ct);
        var text = await resp.Content.ReadAsStringAsync(ct);
        if (string.IsNullOrEmpty(text) || text[0] != '{')
        {
            return ParseInfluxCsv(text, carried);
        }

        using var doc = JsonDocument.Parse(text);
        var output = new List<Sample>();
        if (!doc.RootElement.TryGetProperty("results", out var results))
        {
            return output;
        }

        foreach (var result in results.EnumerateArray())
        {
            if (result.TryGetProperty("error", out var err) && err.ValueKind == JsonValueKind.String)
            {
                throw new InvalidOperationException("influxql: " + err.GetString());
            }

            if (!result.TryGetProperty("series", out var seriesEl) || seriesEl.ValueKind != JsonValueKind.Array)
            {
                continue;
            }

            foreach (var series in seriesEl.EnumerateArray())
            {
                uint tagId = 0;
                if (series.TryGetProperty("tags", out var tags) && tags.TryGetProperty("tag_id", out var idEl))
                {
                    uint.TryParse(idEl.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out tagId);
                }

                var col = new Dictionary<string, int>(StringComparer.Ordinal);
                var i = 0;
                foreach (var name in series.GetProperty("columns").EnumerateArray())
                {
                    col[name.GetString() ?? ""] = i++;
                }

                if (!col.TryGetValue("time", out var ti) || !col.TryGetValue("value", out var vi))
                {
                    continue;
                }

                col.TryGetValue("quality", out var qi);
                var hasQ = col.ContainsKey("quality");
                if (!series.TryGetProperty("values", out var values))
                {
                    continue;
                }

                foreach (var row in values.EnumerateArray())
                {
                    var ts = DateTimeOffset.FromUnixTimeMilliseconds(JsonMs(row[ti]));
                    var value = row[vi].ValueKind == JsonValueKind.Number ? row[vi].GetDouble() : 0;
                    ushort quality = 0;
                    if (hasQ && qi < row.GetArrayLength() && row[qi].ValueKind == JsonValueKind.Number)
                    {
                        quality = (ushort)row[qi].GetDouble();
                    }

                    output.Add(new Sample
                    {
                        Ts = ts,
                        TagId = tagId,
                        Value = value,
                        Quality = quality,
                        Carried = carried
                    });
                }
            }
        }

        return output;
    }

    private async Task EnsureDbrp(CancellationToken ct)
    {
        using var listed = await _http.GetAsync("api/v2/dbrps?org=" + Uri.EscapeDataString(_org) + "&db=" + Uri.EscapeDataString(_bucket), ct);
        if (listed.IsSuccessStatusCode)
        {
            await using var stream = await listed.Content.ReadAsStreamAsync(ct);
            using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: ct);
            if (doc.RootElement.TryGetProperty("content", out var content) && content.GetArrayLength() > 0)
            {
                return;
            }
        }

        using var buckets = await _http.GetAsync("api/v2/buckets?org=" + Uri.EscapeDataString(_org) + "&name=" + Uri.EscapeDataString(_bucket), ct);
        await StoreUtil.EnsureSuccess(buckets, "influx buckets", ct);
        await using var bucketStream = await buckets.Content.ReadAsStreamAsync(ct);
        using var bucketDoc = await JsonDocument.ParseAsync(bucketStream, cancellationToken: ct);
        var id = bucketDoc.RootElement.GetProperty("buckets")[0].GetProperty("id").GetString();
        using var body = JsonContent.Create(new
        {
            org = _org,
            bucketID = id,
            database = _bucket,
            retention_policy = "autogen",
            @default = true
        });
        using var created = await _http.PostAsync("api/v2/dbrps?org=" + Uri.EscapeDataString(_org), body, ct);
        if ((int)created.StatusCode >= 300 && created.StatusCode != System.Net.HttpStatusCode.Conflict)
        {
            await StoreUtil.EnsureSuccess(created, "influx dbrp", ct);
        }
    }

    private static IReadOnlyList<Sample> ParseInfluxCsv(string text, bool carried)
    {
        var output = new List<Sample>();
        if (string.IsNullOrWhiteSpace(text))
        {
            return output;
        }

        Dictionary<string, int>? idx = null;
        using var reader = new StringReader(text);
        while (reader.ReadLine() is { } line)
        {
            if (line.Length == 0 || line[0] == '#')
            {
                continue;
            }

            var parts = line.Split(',');
            if (idx is null)
            {
                var candidate = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                for (var i = 0; i < parts.Length; i++)
                {
                    candidate[parts[i].Trim().Trim('"')] = i;
                }

                if (!candidate.ContainsKey("time"))
                {
                    continue;
                }

                idx = candidate;
                continue;
            }

            if (!idx.TryGetValue("time", out var ti) || !idx.TryGetValue("value", out var vi)
                || ti >= parts.Length || vi >= parts.Length)
            {
                continue;
            }

            uint tagId = 0;
            if (idx.TryGetValue("tag_id", out var idi) && idi < parts.Length)
            {
                uint.TryParse(parts[idi].Trim('"'), NumberStyles.Integer, CultureInfo.InvariantCulture, out tagId);
            }
            else if (idx.TryGetValue("tags", out var tgi) && tgi < parts.Length)
            {
                foreach (var piece in parts[tgi].Trim('"').Split(','))
                {
                    var eq = piece.IndexOf('=');
                    if (eq > 0 && piece.AsSpan(0, eq).Trim().Equals("tag_id", StringComparison.Ordinal))
                    {
                        uint.TryParse(piece[(eq + 1)..], NumberStyles.Integer, CultureInfo.InvariantCulture, out tagId);
                    }
                }
            }

            ushort quality = 0;
            if (idx.TryGetValue("quality", out var qi) && qi < parts.Length)
            {
                if (double.TryParse(parts[qi], NumberStyles.Float, CultureInfo.InvariantCulture, out var qf))
                {
                    quality = (ushort)qf;
                }
            }

            DateTimeOffset ts;
            if (long.TryParse(parts[ti], NumberStyles.Integer, CultureInfo.InvariantCulture, out var ms))
            {
                ts = DateTimeOffset.FromUnixTimeMilliseconds(ms);
            }
            else if (!DateTimeOffset.TryParse(parts[ti].Trim('"'), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out ts))
            {
                continue;
            }

            double.TryParse(parts[vi], NumberStyles.Float, CultureInfo.InvariantCulture, out var val);
            output.Add(new Sample
            {
                Ts = ts,
                TagId = tagId,
                Value = val,
                Quality = quality,
                Carried = carried
            });
        }

        return output;
    }

    private static long JsonMs(JsonElement el)
    {
        if (el.ValueKind != JsonValueKind.Number)
        {
            return 0;
        }

        return el.TryGetInt64(out var n) ? n : (long)el.GetDouble();
    }

    private static string QlTime(DateTimeOffset ts) =>
        "'" + ts.ToUniversalTime().UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", CultureInfo.InvariantCulture) + "'";

    private static string TagRe(IReadOnlyList<uint> ids)
    {
        if (ids.Count == 0)
        {
            return "true";
        }

        return "tag_id =~ /^(" + string.Join('|', ids.Select(id => id.ToString(CultureInfo.InvariantCulture))) + ")$/";
    }
}
