using System.Globalization;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Prism.Api.Models;

namespace Prism.Api.Store;

public sealed class VictoriaMetricsStore : IStore
{
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
            buf.AppendAscii("prism_sample,tag_id=");
            buf.AppendUInt(sample.TagId);
            buf.AppendAscii(",quality=");
            buf.AppendUShort(sample.Quality);
            buf.AppendAscii(" value=");
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

    public async Task<IReadOnlyList<Sample>> LocfAsync(IReadOnlyList<uint> tagIds, DateTimeOffset at, CancellationToken ct = default)
    {
        var output = new List<Sample>();
        foreach (var id in tagIds)
        {
            var qs = $"query={Uri.EscapeDataString($"last_over_time(prism_sample{{tag_id=\"{id}\"}}[30d])")}&time={at.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture)}";
            var parsed = await GetJson<VmInstantResponse>($"/api/v1/query?{qs}", ct);
            if (parsed.Data.Result.Count == 0)
            {
                continue;
            }

            var sample = parsed.Data.Result[0];
            var ts = sample.Value.Count > 0 ? AsFloat(sample.Value[0]) : 0;
            var raw = sample.Value.Count > 1 ? Convert.ToString(sample.Value[1], CultureInfo.InvariantCulture) ?? "0" : "0";
            double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var val);
            ushort quality = 0;
            if (sample.Metric.TryGetValue("quality", out var qRaw) && int.TryParse(qRaw, out var q))
            {
                quality = (ushort)q;
            }

            output.Add(new Sample
            {
                Ts = DateTimeOffset.FromUnixTimeSeconds((long)ts),
                TagId = id,
                Value = val,
                Quality = quality
            });
        }

        return output;
    }

    public async Task<IReadOnlyList<Sample>> RangeAsync(IReadOnlyList<uint> tagIds, DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default)
    {
        var seed = (await LocfAsync(tagIds, from, ct)).ToList();
        foreach (var sample in seed)
        {
            sample.Carried = true;
        }

        var mid = new List<Sample>();
        foreach (var id in tagIds)
        {
            var qs = $"match[]={Uri.EscapeDataString($"prism_sample{{tag_id=\"{id}\"}}")}&start={from.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture)}&end={to.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture)}";
            using var resp = await _http.GetAsync(_base + "/api/v1/export?" + qs, ct);
            await StoreUtil.EnsureSuccess(resp, "vm export", ct);
            await using var stream = await resp.Content.ReadAsStreamAsync(ct);
            using var doc = new StreamReader(stream);
            while (await doc.ReadLineAsync(ct) is { } line)
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                var row = JsonSerializer.Deserialize<VmExportRow>(line, JsonOpts);
                if (row is null)
                {
                    continue;
                }

                ushort quality = 0;
                if (row.Metric.TryGetValue("quality", out var qRaw) && int.TryParse(qRaw, out var q))
                {
                    quality = (ushort)q;
                }

                for (var i = 0; i < row.Timestamps.Count; i++)
                {
                    var t = DateTimeOffset.FromUnixTimeMilliseconds(row.Timestamps[i]);
                    if (t <= from || t > to)
                    {
                        continue;
                    }

                    mid.Add(new Sample
                    {
                        Ts = t,
                        TagId = id,
                        Value = i < row.Values.Count ? row.Values[i] : 0,
                        Quality = quality
                    });
                }
            }
        }

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

    private async Task<T> GetJson<T>(string path, CancellationToken ct)
    {
        using var resp = await _http.GetAsync(_base + path, ct);
        await StoreUtil.EnsureSuccess(resp, "vm query", ct);
        return await resp.Content.ReadFromJsonAsync<T>(JsonOpts, ct)
               ?? throw new InvalidOperationException("vm: empty response");
    }

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private static double AsFloat(object? value) =>
        value switch
        {
            JsonElement el when el.ValueKind == JsonValueKind.Number => el.GetDouble(),
            JsonElement el when el.ValueKind == JsonValueKind.String => double.TryParse(el.GetString(), CultureInfo.InvariantCulture, out var n) ? n : 0,
            double d => d,
            _ => Convert.ToDouble(value, CultureInfo.InvariantCulture)
        };

    private sealed class VmInstantResponse
    {
        public VmData Data { get; set; } = new();
    }

    private sealed class VmData
    {
        public List<VmInstant> Result { get; set; } = [];
    }

    private sealed class VmInstant
    {
        public Dictionary<string, string> Metric { get; set; } = [];
        public List<object?> Value { get; set; } = [];
    }

    private sealed class VmExportRow
    {
        [JsonPropertyName("metric")]
        public Dictionary<string, string> Metric { get; set; } = [];

        [JsonPropertyName("timestamps")]
        public List<long> Timestamps { get; set; } = [];

        [JsonPropertyName("values")]
        public List<double> Values { get; set; } = [];
    }
}
