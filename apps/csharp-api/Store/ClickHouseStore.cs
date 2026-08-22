using System.Buffers.Binary;
using System.Net.Http.Headers;
using System.Text;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;
using Prism.Api.Models;

namespace Prism.Api.Store;

public sealed class ClickHouseStore : IStore
{
    private const string InsertSql =
        "INSERT INTO samples (ts, tag_id, value, quality) " +
        "SELECT fromUnixTimestamp64Milli(ts), tag_id, value, quality " +
        "FROM input('ts Int64, tag_id UInt32, value Float32, quality UInt16') FORMAT RowBinary";
    private const int RowBytes = 18;

    private readonly string _connectionString;
    private readonly HttpClient _http;
    private readonly string _insertPath;

    public ClickHouseStore(string url, string database)
    {
        _connectionString = ToConnectionString(url, database);
        var (origin, user, password) = SplitHttp(url);
        _http = StoreUtil.CreatePooledHttp(TimeSpan.FromSeconds(60), origin);
        if (!string.IsNullOrEmpty(user))
        {
            var token = Convert.ToBase64String(Encoding.ASCII.GetBytes(user + ":" + password));
            _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", token);
        }

        _insertPath = "/?database=" + Uri.EscapeDataString(database) + "&query=" + Uri.EscapeDataString(InsertSql)
                      + "&async_insert=1&wait_for_async_insert=1&async_insert_busy_timeout_ms=200"
                      + "&async_insert_max_data_size=1048576";
    }

    public string Name => "clickhouse";

    public async Task PingAsync(CancellationToken ct = default)
    {
        using var resp = await _http.GetAsync("/?query=SELECT%201", ct);
        await StoreUtil.EnsureSuccess(resp, "clickhouse ping", ct);
    }

    public async Task WriteAsync(IReadOnlyList<Sample> samples, CancellationToken ct = default)
    {
        if (samples.Count == 0)
        {
            return;
        }

        var payload = EncodeRowBinary(samples);
        Exception? last = null;
        for (var attempt = 0; attempt < 2; attempt++)
        {
            try
            {
                using var content = new ByteArrayContent(payload);
                content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
                using var resp = await _http.PostAsync(_insertPath, content, ct);
                if (attempt == 0 && (int)resp.StatusCode >= 500)
                {
                    last = new InvalidOperationException(
                        $"clickhouse write status {(int)resp.StatusCode}: {await resp.Content.ReadAsStringAsync(ct)}");
                    continue;
                }

                await StoreUtil.EnsureSuccess(resp, "clickhouse write", ct);
                return;
            }
            catch (Exception ex) when (attempt == 0 && StoreUtil.IsTransient(ex))
            {
                last = ex;
            }
        }

        throw last ?? new InvalidOperationException("clickhouse write failed");
    }

    public async Task<IReadOnlyList<Sample>> LocfAsync(IReadOnlyList<uint> tagIds, DateTimeOffset at, CancellationToken ct = default)
    {
        var rows = await LocfQuery(tagIds, at, bounded: true, ct);
        var missing = MissingTagIds(tagIds, rows);
        if (missing.Count == 0)
        {
            return rows;
        }

        var rest = await LocfQuery(missing, at, bounded: false, ct);
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
        var head = await LocfAsync(tagIds, from, ct);
        await using var conn = await Open(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            """
            SELECT s.ts, s.tag_id, s.value, s.quality
            FROM samples AS s
            WHERE s.tag_id IN {ids:Array(UInt32)} AND s.ts > {from:DateTime64} AND s.ts <= {to:DateTime64}
            """;
        cmd.AddParameter("ids", tagIds.ToArray());
        cmd.AddParameter("from", from.UtcDateTime);
        cmd.AddParameter("to", to.UtcDateTime);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var tail = await Scan(reader, carried: false, ct);
        return MergeRange(tagIds, head, tail);
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

    private async Task<IReadOnlyList<Sample>> LocfQuery(IReadOnlyList<uint> tagIds, DateTimeOffset at, bool bounded, CancellationToken ct)
    {
        await using var conn = await Open(ct);
        await using var cmd = conn.CreateCommand();
        if (bounded)
        {
            cmd.CommandText =
                """
                SELECT max(s.ts) AS ts, s.tag_id, argMax(s.value, s.ts) AS value, argMax(s.quality, s.ts) AS quality
                FROM samples AS s
                WHERE s.tag_id IN {ids:Array(UInt32)} AND s.ts <= {at:DateTime64} AND s.ts >= {since:DateTime64}
                GROUP BY s.tag_id
                """;
            cmd.AddParameter("since", at.UtcDateTime.AddHours(-3));
        }
        else
        {
            cmd.CommandText =
                """
                SELECT max(s.ts) AS ts, s.tag_id, argMax(s.value, s.ts) AS value, argMax(s.quality, s.ts) AS quality
                FROM samples AS s
                WHERE s.tag_id IN {ids:Array(UInt32)} AND s.ts <= {at:DateTime64}
                GROUP BY s.tag_id
                """;
        }

        cmd.AddParameter("ids", tagIds.ToArray());
        cmd.AddParameter("at", at.UtcDateTime);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await Scan(reader, carried: false, ct);
    }

    private static List<uint> MissingTagIds(IReadOnlyList<uint> tagIds, IReadOnlyList<Sample> rows)
    {
        var found = new HashSet<uint>(rows.Select(s => s.TagId));
        var missing = new List<uint>();
        var seen = new HashSet<uint>();
        foreach (var id in tagIds)
        {
            if (!seen.Add(id))
            {
                continue;
            }

            if (!found.Contains(id))
            {
                missing.Add(id);
            }
        }

        return missing;
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

    public async Task UpsertTagsAsync(IReadOnlyList<Tag> tags, CancellationToken ct = default)
    {
        if (tags.Count == 0)
        {
            return;
        }

        await using var conn = await Open(ct);
        using var bulk = new ClickHouseBulkCopy(conn)
        {
            DestinationTableName = "tags",
            ColumnNames = ["id", "name", "unit"],
            BatchSize = tags.Count
        };
        await bulk.InitAsync();
        await bulk.WriteToServerAsync(tags.Select(t => new object[]
        {
            t.Id,
            t.Name,
            t.Unit ?? ""
        }), ct);
    }

    public async Task<IReadOnlyList<Tag>> ListTagsAsync(CancellationToken ct = default)
    {
        await using var conn = await Open(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, name, unit FROM tags ORDER BY id";
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var output = new List<Tag>();
        while (await reader.ReadAsync(ct))
        {
            output.Add(new Tag
            {
                Id = Convert.ToUInt32(reader.GetValue(0)),
                Name = reader.GetString(1),
                Unit = reader.IsDBNull(2) ? "" : Convert.ToString(reader.GetValue(2)) ?? ""
            });
        }

        return output;
    }

    public ValueTask DisposeAsync()
    {
        _http.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task<ClickHouseConnection> Open(CancellationToken ct)
    {
        var conn = new ClickHouseConnection(_connectionString);
        await conn.OpenAsync(ct);
        return conn;
    }

    private static async Task<IReadOnlyList<Sample>> Scan(System.Data.Common.DbDataReader reader, bool? carried, CancellationToken ct)
    {
        var output = new List<Sample>();
        while (await reader.ReadAsync(ct))
        {
            output.Add(new Sample
            {
                Ts = ToUtc(reader.GetDateTime(0)),
                TagId = Convert.ToUInt32(reader.GetValue(1)),
                Value = Convert.ToDouble(reader.GetValue(2)),
                Quality = Convert.ToUInt16(reader.GetValue(3)),
                Carried = carried ?? Convert.ToInt32(reader.GetValue(4)) != 0
            });
        }

        return output;
    }

    private static DateTimeOffset ToUtc(DateTime dt) =>
        new(DateTime.SpecifyKind(dt.Kind == DateTimeKind.Unspecified ? dt : dt.ToUniversalTime(), DateTimeKind.Utc));

    private static byte[] EncodeRowBinary(IReadOnlyList<Sample> samples)
    {
        var payload = new byte[RowBytes * samples.Count];
        var offset = 0;
        foreach (var sample in samples)
        {
            var span = payload.AsSpan(offset, RowBytes);
            BinaryPrimitives.WriteInt64LittleEndian(span, sample.Ts.ToUnixTimeMilliseconds());
            BinaryPrimitives.WriteUInt32LittleEndian(span[8..], sample.TagId);
            BinaryPrimitives.WriteSingleLittleEndian(span[12..], (float)sample.Value);
            BinaryPrimitives.WriteUInt16LittleEndian(span[16..], sample.Quality);
            offset += RowBytes;
        }

        return payload;
    }

    private static (string Origin, string User, string Password) SplitHttp(string url)
    {
        if (!url.Contains("://", StringComparison.Ordinal))
        {
            return ("http://clickhouse:8123/", "prism", "prism");
        }

        var uri = new Uri(url);
        var user = "default";
        var password = "";
        if (!string.IsNullOrEmpty(uri.UserInfo))
        {
            var parts = uri.UserInfo.Split(':', 2);
            user = Uri.UnescapeDataString(parts[0]);
            if (parts.Length > 1)
            {
                password = Uri.UnescapeDataString(parts[1]);
            }
        }

        var port = uri.IsDefaultPort ? 8123 : uri.Port;
        return ($"{uri.Scheme}://{uri.Host}:{port}/", user, password);
    }

    internal static string ToConnectionString(string url, string database)
    {
        if (!url.Contains("://", StringComparison.Ordinal))
        {
            return url.Contains("Database=", StringComparison.OrdinalIgnoreCase)
                ? url
                : url.TrimEnd(';') + $";Database={database}";
        }

        var uri = new Uri(url);
        var user = "default";
        var password = "";
        if (!string.IsNullOrEmpty(uri.UserInfo))
        {
            var parts = uri.UserInfo.Split(':', 2);
            user = Uri.UnescapeDataString(parts[0]);
            if (parts.Length > 1)
            {
                password = Uri.UnescapeDataString(parts[1]);
            }
        }

        var port = uri.IsDefaultPort ? 8123 : uri.Port;
        var protocol = uri.Scheme.Equals("https", StringComparison.OrdinalIgnoreCase) ? "https" : "http";
        return $"Host={uri.Host};Port={port};Protocol={protocol};Username={user};Password={password};Database={database}";
    }
}
