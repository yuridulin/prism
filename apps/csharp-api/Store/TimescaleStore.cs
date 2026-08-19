using Npgsql;
using Prism.Api.Models;

namespace Prism.Api.Store;

public sealed class TimescaleStore : IStore
{
    private readonly NpgsqlDataSource _ds;

    public TimescaleStore(string dsn)
    {
        var builder = new NpgsqlDataSourceBuilder(dsn);
        builder.ConnectionStringBuilder.MaxPoolSize = 16;
        _ds = builder.Build();
    }

    public string Name => "timescaledb";

    public async Task PingAsync(CancellationToken ct = default)
    {
        await using var conn = await _ds.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("SELECT 1", conn);
        await cmd.ExecuteScalarAsync(ct);
    }

    public async Task WriteAsync(IReadOnlyList<Sample> samples, CancellationToken ct = default)
    {
        if (samples.Count == 0)
        {
            return;
        }

        await using var conn = await _ds.OpenConnectionAsync(ct);
        await using var batch = new NpgsqlBatch(conn);
        foreach (var sample in samples)
        {
            var cmd = new NpgsqlBatchCommand("INSERT INTO samples (ts, tag_id, value, quality) VALUES ($1, $2, $3, $4)");
            cmd.Parameters.Add(new NpgsqlParameter { Value = sample.Ts.UtcDateTime });
            cmd.Parameters.Add(new NpgsqlParameter { Value = unchecked((int)sample.TagId) });
            cmd.Parameters.Add(new NpgsqlParameter { Value = (float)sample.Value });
            cmd.Parameters.Add(new NpgsqlParameter { Value = (short)sample.Quality });
            batch.BatchCommands.Add(cmd);
        }

        await batch.ExecuteNonQueryAsync(ct);
    }

    public async Task<IReadOnlyList<Sample>> LocfAsync(IReadOnlyList<uint> tagIds, DateTimeOffset at, CancellationToken ct = default)
    {
        await using var conn = await _ds.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            """
            SELECT DISTINCT ON (tag_id) ts, tag_id, value, quality
            FROM samples
            WHERE tag_id = ANY($1) AND ts <= $2
            ORDER BY tag_id, ts DESC
            """, conn);
        cmd.Parameters.Add(new NpgsqlParameter { Value = StoreUtil.IntTags(tagIds) });
        cmd.Parameters.Add(new NpgsqlParameter { Value = at.UtcDateTime });
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await ScanSamples(reader, carried: false, ct);
    }

    public async Task<IReadOnlyList<Sample>> RangeAsync(IReadOnlyList<uint> tagIds, DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default)
    {
        await using var conn = await _ds.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            """
            SELECT ts, tag_id, value, quality, carried FROM (
                SELECT DISTINCT ON (tag_id) ts, tag_id, value, quality, true AS carried
                FROM samples
                WHERE tag_id = ANY($1) AND ts <= $2
                ORDER BY tag_id, ts DESC
                UNION ALL
                SELECT ts, tag_id, value, quality, false
                FROM samples
                WHERE tag_id = ANY($1) AND ts > $2 AND ts <= $3
            ) s
            ORDER BY tag_id, ts
            """, conn);
        var ids = StoreUtil.IntTags(tagIds);
        cmd.Parameters.Add(new NpgsqlParameter { Value = ids });
        cmd.Parameters.Add(new NpgsqlParameter { Value = from.UtcDateTime });
        cmd.Parameters.Add(new NpgsqlParameter { Value = to.UtcDateTime });
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await ScanSamples(reader, carried: null, ct);
    }

    public async Task UpsertTagsAsync(IReadOnlyList<Tag> tags, CancellationToken ct = default)
    {
        if (tags.Count == 0)
        {
            return;
        }

        await using var conn = await _ds.OpenConnectionAsync(ct);
        await using var batch = new NpgsqlBatch(conn);
        foreach (var tag in tags)
        {
            var cmd = new NpgsqlBatchCommand(
                """
                INSERT INTO tags (id, name, unit) VALUES ($1, $2, $3)
                ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, unit = EXCLUDED.unit
                """);
            cmd.Parameters.Add(new NpgsqlParameter { Value = unchecked((int)tag.Id) });
            cmd.Parameters.Add(new NpgsqlParameter { Value = tag.Name });
            cmd.Parameters.Add(new NpgsqlParameter { Value = (object?)tag.Unit ?? "" });
            batch.BatchCommands.Add(cmd);
        }

        await batch.ExecuteNonQueryAsync(ct);
    }

    public async Task<IReadOnlyList<Tag>> ListTagsAsync(CancellationToken ct = default)
    {
        await using var conn = await _ds.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("SELECT id, name, COALESCE(unit, '') FROM tags ORDER BY id", conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var output = new List<Tag>();
        while (await reader.ReadAsync(ct))
        {
            output.Add(new Tag
            {
                Id = (uint)reader.GetInt32(0),
                Name = reader.GetString(1),
                Unit = reader.GetString(2)
            });
        }

        return output;
    }

    public ValueTask DisposeAsync() => _ds.DisposeAsync();

    private static async Task<IReadOnlyList<Sample>> ScanSamples(NpgsqlDataReader reader, bool? carried, CancellationToken ct)
    {
        var output = new List<Sample>();
        while (await reader.ReadAsync(ct))
        {
            var sample = new Sample
            {
                Ts = ReadTs(reader, 0),
                TagId = (uint)reader.GetInt32(1),
                Value = reader.GetFloat(2),
                Quality = (ushort)reader.GetInt16(3),
                Carried = carried ?? reader.GetBoolean(4)
            };
            output.Add(sample);
        }

        return output;
    }

    private static DateTimeOffset ReadTs(NpgsqlDataReader reader, int ordinal)
    {
        if (reader.GetFieldType(ordinal) == typeof(DateTimeOffset))
        {
            return reader.GetFieldValue<DateTimeOffset>(ordinal).ToUniversalTime();
        }

        var dt = reader.GetDateTime(ordinal);
        return dt.Kind == DateTimeKind.Unspecified
            ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            : new DateTimeOffset(dt.ToUniversalTime());
    }
}
