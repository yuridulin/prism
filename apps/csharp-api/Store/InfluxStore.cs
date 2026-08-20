using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using Prism.Api.Models;

namespace Prism.Api.Store;

public sealed class InfluxStore : IStore
{
    private readonly HttpClient _http;
    private readonly string _org;
    private readonly string _bucket;
    private readonly string _writePath;
    private readonly CatalogMem _tags = new();
    // Archive max gap is 1h; 3h still finds the previous minute/hour point at 364d ago.
    private static readonly TimeSpan LocfLookback = TimeSpan.FromHours(3);

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
        var flux = $"""
            from(bucket: "{_bucket}")
              |> range(start: {StoreUtil.Rfc3339Nano(stop - LocfLookback)}, stop: {StoreUtil.Rfc3339Nano(stop.AddTicks(1))})
              |> filter(fn: (r) => r._measurement == "samples")
              |> filter(fn: (r) => {TagFilter(tagIds)})
              |> last()
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            """;
        return await Collect(flux, carried, ct);
    }

    private async Task<IReadOnlyList<Sample>> QueryWindow(IReadOnlyList<uint> tagIds, DateTimeOffset from, DateTimeOffset to, CancellationToken ct)
    {
        var flux = $"""
            from(bucket: "{_bucket}")
              |> range(start: {StoreUtil.Rfc3339Nano(from.AddTicks(1))}, stop: {StoreUtil.Rfc3339Nano(to.AddTicks(1))})
              |> filter(fn: (r) => r._measurement == "samples")
              |> filter(fn: (r) => {TagFilter(tagIds)})
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            """;
        return await Collect(flux, carried: false, ct);
    }

    private async Task<IReadOnlyList<Sample>> Collect(string flux, bool carried, CancellationToken ct)
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, "api/v2/query?org=" + Uri.EscapeDataString(_org))
        {
            Content = new StringContent(flux, Encoding.UTF8, "application/vnd.flux")
        };
        req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/csv"));
        using var resp = await _http.SendAsync(req, ct);
        await StoreUtil.EnsureSuccess(resp, "influxdb query", ct);
        var csv = await resp.Content.ReadAsStringAsync(ct);
        return ParseFluxCsv(csv, carried);
    }

    internal static List<Sample> ParseFluxCsv(string csv, bool carried)
    {
        var output = new List<Sample>();
        string[]? headers = null;
        using var reader = new StringReader(csv);
        while (reader.ReadLine() is { } line)
        {
            if (line.StartsWith('#') || string.IsNullOrWhiteSpace(line))
            {
                if (line.StartsWith('#'))
                {
                    headers = null;
                }

                continue;
            }

            var cols = SplitCsv(line);
            if (headers is null)
            {
                headers = cols;
                continue;
            }

            var map = new Dictionary<string, string>(StringComparer.Ordinal);
            for (var i = 0; i < headers.Length && i < cols.Length; i++)
            {
                if (!string.IsNullOrEmpty(headers[i]))
                {
                    map[headers[i]] = cols[i];
                }
            }

            if (!map.TryGetValue("_time", out var timeRaw) || string.IsNullOrEmpty(timeRaw))
            {
                continue;
            }

            if (!DateTimeOffset.TryParse(timeRaw, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var ts))
            {
                continue;
            }

            map.TryGetValue("tag_id", out var tagRaw);
            map.TryGetValue("value", out var valueRaw);
            map.TryGetValue("quality", out var qualityRaw);
            uint.TryParse(tagRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var tagId);
            double.TryParse(valueRaw, NumberStyles.Float, CultureInfo.InvariantCulture, out var value);
            ushort.TryParse(qualityRaw?.Split('.')[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out var quality);
            output.Add(new Sample
            {
                Ts = ts,
                TagId = tagId,
                Value = value,
                Quality = quality,
                Carried = carried
            });
        }

        return output;
    }

    private static string TagFilter(IReadOnlyList<uint> ids)
    {
        if (ids.Count == 0)
        {
            return "true";
        }

        return string.Join(" or ", ids.Select(id => $"r.tag_id == \"{id.ToString(CultureInfo.InvariantCulture)}\""));
    }

    private static string[] SplitCsv(string line)
    {
        var cols = new List<string>();
        var sb = new StringBuilder();
        var inQuotes = false;
        for (var i = 0; i < line.Length; i++)
        {
            var c = line[i];
            if (inQuotes)
            {
                if (c == '"' && i + 1 < line.Length && line[i + 1] == '"')
                {
                    sb.Append('"');
                    i++;
                }
                else if (c == '"')
                {
                    inQuotes = false;
                }
                else
                {
                    sb.Append(c);
                }
            }
            else if (c == '"')
            {
                inQuotes = true;
            }
            else if (c == ',')
            {
                cols.Add(sb.ToString());
                sb.Clear();
            }
            else
            {
                sb.Append(c);
            }
        }

        cols.Add(sb.ToString());
        return cols.ToArray();
    }
}
