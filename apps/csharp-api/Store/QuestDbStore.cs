using System.Globalization;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Prism.Api.Models;

namespace Prism.Api.Store;

public sealed class QuestDbStore : IStore
{
    private readonly string _base;
    private readonly HttpClient _http;

    public QuestDbStore(string url)
    {
        _base = url.TrimEnd('/');
        _http = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
    }

    public string Name => "questdb";

    public Task PingAsync(CancellationToken ct = default) => Exec(ct, "SELECT 1").AsTask();

    public async Task WriteAsync(IReadOnlyList<Sample> samples, CancellationToken ct = default)
    {
        if (samples.Count == 0)
        {
            return;
        }

        var sb = new StringBuilder();
        foreach (var sample in samples)
        {
            sb.Append(CultureInfo.InvariantCulture,
                $"samples tag_id={sample.TagId}i,value={StoreUtil.FormatFloat(sample.Value)},quality={sample.Quality}i {StoreUtil.UnixNano(sample.Ts)}\n");
        }

        using var req = new HttpRequestMessage(HttpMethod.Post, _base + "/write?precision=n")
        {
            Content = new StringContent(sb.ToString(), Encoding.UTF8, "text/plain")
        };
        using var resp = await _http.SendAsync(req, ct);
        await StoreUtil.EnsureSuccess(resp, "questdb write", ct);
    }

    public async Task<IReadOnlyList<Sample>> LocfAsync(IReadOnlyList<uint> tagIds, DateTimeOffset at, CancellationToken ct = default)
    {
        var q =
            $"SELECT ts, tag_id, value, quality FROM samples WHERE tag_id IN ({StoreUtil.JoinIds(tagIds)}) AND ts <= '{StoreUtil.QuestDbTime(at)}' LATEST ON ts PARTITION BY tag_id";
        var data = await Exec(ct, q);
        return ParseSamples(data, hasCarried: false);
    }

    public async Task<IReadOnlyList<Sample>> RangeAsync(IReadOnlyList<uint> tagIds, DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default)
    {
        var ids = StoreUtil.JoinIds(tagIds);
        var q = $"""
            SELECT ts, tag_id, value, quality, carried FROM (
                SELECT ts, tag_id, value, quality, true AS carried
                FROM samples
                WHERE tag_id IN ({ids}) AND ts <= '{StoreUtil.QuestDbTime(from)}'
                LATEST ON ts PARTITION BY tag_id
                UNION ALL
                SELECT ts, tag_id, value, quality, false
                FROM samples
                WHERE tag_id IN ({ids}) AND ts > '{StoreUtil.QuestDbTime(from)}' AND ts <= '{StoreUtil.QuestDbTime(to)}'
            )
            ORDER BY tag_id, ts
            """;
        var data = await Exec(ct, q);
        return ParseSamples(data, hasCarried: true);
    }

    public async Task UpsertTagsAsync(IReadOnlyList<Tag> tags, CancellationToken ct = default)
    {
        foreach (var tag in tags)
        {
            var name = (tag.Name ?? "").Replace("'", "''", StringComparison.Ordinal);
            var unit = (tag.Unit ?? "").Replace("'", "''", StringComparison.Ordinal);
            await Exec(ct, $"INSERT INTO tags (id, name, unit) VALUES ({tag.Id}, '{name}', '{unit}')");
        }
    }

    public async Task<IReadOnlyList<Tag>> ListTagsAsync(CancellationToken ct = default)
    {
        var data = await Exec(ct, "SELECT id, name, unit FROM tags ORDER BY id");
        var output = new List<Tag>();
        foreach (var row in data.Dataset)
        {
            if (row.Count < 3)
            {
                continue;
            }

            output.Add(new Tag
            {
                Id = (uint)AsFloat(row[0]),
                Name = Convert.ToString(row[1]) ?? "",
                Unit = Convert.ToString(row[2]) ?? ""
            });
        }

        return output;
    }

    public ValueTask DisposeAsync()
    {
        _http.Dispose();
        return ValueTask.CompletedTask;
    }

    private async ValueTask<QuestDbExec> Exec(CancellationToken ct, string query)
    {
        var url = _base + "/exec?query=" + Uri.EscapeDataString(query);
        using var resp = await _http.GetAsync(url, ct);
        var data = await resp.Content.ReadFromJsonAsync<QuestDbExec>(JsonOpts, ct)
                   ?? throw new InvalidOperationException("questdb: empty response");
        if (!string.IsNullOrEmpty(data.Error))
        {
            throw new InvalidOperationException("questdb: " + data.Error);
        }

        return data;
    }

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private static List<Sample> ParseSamples(QuestDbExec data, bool hasCarried)
    {
        var output = new List<Sample>();
        foreach (var row in data.Dataset)
        {
            if (row.Count < 4)
            {
                continue;
            }

            var sample = new Sample
            {
                Ts = ParseTs(row[0]),
                TagId = (uint)AsFloat(row[1]),
                Value = AsFloat(row[2]),
                Quality = (ushort)AsFloat(row[3])
            };
            if (hasCarried && row.Count > 4)
            {
                sample.Carried = AsBool(row[4]);
            }

            output.Add(sample);
        }

        return output;
    }

    private static DateTimeOffset ParseTs(object? value) =>
        value switch
        {
            string s when DateTimeOffset.TryParseExact(s, "yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var ts) => ts,
            string s when DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var ts) => ts,
            JsonElement { ValueKind: JsonValueKind.String } el when DateTimeOffset.TryParse(el.GetString(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var ts) => ts,
            JsonElement { ValueKind: JsonValueKind.Number } el => DateTimeOffset.FromUnixTimeMilliseconds(el.GetInt64()),
            double d => DateTimeOffset.FromUnixTimeMilliseconds((long)d),
            long l => DateTimeOffset.FromUnixTimeMilliseconds(l),
            _ => throw new InvalidOperationException($"questdb ts {value?.GetType()}")
        };

    private static double AsFloat(object? value) =>
        value switch
        {
            null => 0,
            double d => d,
            float f => f,
            int i => i,
            long l => l,
            decimal m => (double)m,
            string s => double.TryParse(s, CultureInfo.InvariantCulture, out var n) ? n : 0,
            JsonElement el => el.ValueKind switch
            {
                JsonValueKind.Number => el.GetDouble(),
                JsonValueKind.String => double.TryParse(el.GetString(), CultureInfo.InvariantCulture, out var n) ? n : 0,
                _ => 0
            },
            _ => Convert.ToDouble(value, CultureInfo.InvariantCulture)
        };

    private static bool AsBool(object? value) =>
        value switch
        {
            bool b => b,
            double d => d != 0,
            long l => l != 0,
            int i => i != 0,
            string s => s is "true" or "t" or "1",
            JsonElement el => el.ValueKind switch
            {
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Number => el.GetDouble() != 0,
                JsonValueKind.String => el.GetString() is "true" or "t" or "1",
                _ => false
            },
            _ => false
        };

    private sealed class QuestDbExec
    {
        public string? Query { get; set; }
        public List<List<object?>> Dataset { get; set; } = [];
        public string? Error { get; set; }
    }
}
