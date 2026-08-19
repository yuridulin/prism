using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;
using Prism.Api.Models;

namespace Prism.Api.Store;

public sealed class ClickHouseStore : IStore
{
    private readonly string _connectionString;

    public ClickHouseStore(string url, string database)
    {
        _connectionString = ToConnectionString(url, database);
    }

    public string Name => "clickhouse";

    public async Task PingAsync(CancellationToken ct = default)
    {
        await using var conn = await Open(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        _ = await cmd.ExecuteScalarAsync(ct);
    }

    public async Task WriteAsync(IReadOnlyList<Sample> samples, CancellationToken ct = default)
    {
        if (samples.Count == 0)
        {
            return;
        }

        await using var conn = await Open(ct);
        using var bulk = new ClickHouseBulkCopy(conn)
        {
            DestinationTableName = "samples",
            ColumnNames = ["ts", "tag_id", "value", "quality"],
            BatchSize = samples.Count
        };
        await bulk.InitAsync();
        await bulk.WriteToServerAsync(samples.Select(s => new object[]
        {
            s.Ts.UtcDateTime,
            s.TagId,
            (float)s.Value,
            s.Quality
        }), ct);
    }

    public async Task<IReadOnlyList<Sample>> LocfAsync(IReadOnlyList<uint> tagIds, DateTimeOffset at, CancellationToken ct = default)
    {
        await using var conn = await Open(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            """
            SELECT ts, tag_id, value, quality
            FROM samples
            WHERE tag_id IN {ids:Array(UInt32)} AND ts <= {at:DateTime64}
            ORDER BY tag_id, ts DESC
            LIMIT 1 BY tag_id
            """;
        cmd.AddParameter("ids", tagIds.ToArray());
        cmd.AddParameter("at", at.UtcDateTime);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await Scan(reader, carried: false, ct);
    }

    public async Task<IReadOnlyList<Sample>> RangeAsync(IReadOnlyList<uint> tagIds, DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default)
    {
        await using var conn = await Open(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            """
            SELECT ts, tag_id, value, quality, carried FROM (
                SELECT ts, tag_id, value, quality, 1 AS carried
                FROM samples
                WHERE tag_id IN {ids:Array(UInt32)} AND ts <= {from:DateTime64}
                ORDER BY tag_id, ts DESC
                LIMIT 1 BY tag_id
                UNION ALL
                SELECT ts, tag_id, value, quality, 0
                FROM samples
                WHERE tag_id IN {ids2:Array(UInt32)} AND ts > {from2:DateTime64} AND ts <= {to:DateTime64}
            )
            ORDER BY tag_id, ts
            """;
        var ids = tagIds.ToArray();
        cmd.AddParameter("ids", ids);
        cmd.AddParameter("from", from.UtcDateTime);
        cmd.AddParameter("ids2", ids);
        cmd.AddParameter("from2", from.UtcDateTime);
        cmd.AddParameter("to", to.UtcDateTime);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await Scan(reader, carried: null, ct);
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

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

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
