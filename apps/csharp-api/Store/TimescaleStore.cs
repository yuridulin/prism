using Npgsql;
using NpgsqlTypes;
using Prism.Api.Models;

namespace Prism.Api.Store;

public sealed class TimescaleStore : IStore
{
    private readonly NpgsqlDataSource? _ds;
    private readonly string? _initError;

    public TimescaleStore(string dsn)
    {
        try
        {
            var builder = new NpgsqlDataSourceBuilder(AppConfig.ToNpgsql(dsn));
            var cs = builder.ConnectionStringBuilder;
            cs.MaxPoolSize = 16;
            cs.MinPoolSize = 0;
            cs.Timeout = 15;
            cs.CommandTimeout = 60;
            cs.Pooling = true;
            cs.NoResetOnClose = true;
            cs.Enlist = false;
            _ds = builder.Build();
        }
        catch (Exception ex)
        {
            _initError = ex.Message;
        }
    }

    public string Name => "timescaledb";

    public async Task PingAsync(CancellationToken ct = default)
    {
        await using var conn = await Open(ct);
        await using var cmd = new NpgsqlCommand("SELECT 1", conn);
        await cmd.ExecuteScalarAsync(ct);
    }

    public async Task WriteAsync(IReadOnlyList<Sample> samples, CancellationToken ct = default)
    {
        if (samples.Count == 0)
        {
            return;
        }

        await using var conn = await Open(ct);
        await using var copy = await conn.BeginBinaryImportAsync(
            "COPY samples (ts, tag_id, value, quality) FROM STDIN (FORMAT BINARY)", ct);
        foreach (var sample in samples)
        {
            copy.StartRow();
            copy.Write(sample.Ts.ToUniversalTime(), NpgsqlDbType.TimestampTz);
            copy.Write(unchecked((int)sample.TagId), NpgsqlDbType.Integer);
            copy.Write((float)sample.Value, NpgsqlDbType.Real);
            copy.Write((short)sample.Quality, NpgsqlDbType.Smallint);
        }

        await copy.CompleteAsync(ct);
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
        await using var conn = await Open(ct);
        await using var cmd = new NpgsqlCommand(
            """
            SELECT s.ts, s.tag_id, s.value, s.quality, true AS carried
            FROM unnest($1::int4[]) AS t(tag_id)
            CROSS JOIN LATERAL (
                SELECT ts, tag_id, value, quality
                FROM samples
                WHERE samples.tag_id = t.tag_id AND ts <= $2
                ORDER BY ts DESC
                LIMIT 1
            ) s
            UNION ALL
            SELECT ts, tag_id, value, quality, false
            FROM samples
            WHERE tag_id = ANY($1) AND ts > $2 AND ts <= $3
            """, conn);
        cmd.Parameters.Add(new NpgsqlParameter { Value = StoreUtil.IntTags(tagIds) });
        cmd.Parameters.Add(new NpgsqlParameter { Value = from.UtcDateTime });
        cmd.Parameters.Add(new NpgsqlParameter { Value = to.UtcDateTime });
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var all = await ScanSamples(reader, carried: null, ct);
        var head = new List<Sample>(tagIds.Count);
        var tail = new List<Sample>(all.Count);
        foreach (var sample in all)
        {
            if (sample.Carried)
            {
                head.Add(sample);
            }
            else
            {
                tail.Add(sample);
            }
        }

        return MergeRange(tagIds, head, tail);
    }

    private async Task<IReadOnlyList<Sample>> LocfQuery(IReadOnlyList<uint> tagIds, DateTimeOffset at, bool bounded, CancellationToken ct)
    {
        await using var conn = await Open(ct);
        await using var cmd = new NpgsqlCommand(
            bounded
                ? """
                  SELECT s.ts, s.tag_id, s.value, s.quality
                  FROM unnest($1::int4[]) AS t(tag_id)
                  CROSS JOIN LATERAL (
                      SELECT ts, tag_id, value, quality
                      FROM samples
                      WHERE samples.tag_id = t.tag_id AND ts <= $2 AND ts >= $3
                      ORDER BY ts DESC
                      LIMIT 1
                  ) s
                  """
                : """
                  SELECT s.ts, s.tag_id, s.value, s.quality
                  FROM unnest($1::int4[]) AS t(tag_id)
                  CROSS JOIN LATERAL (
                      SELECT ts, tag_id, value, quality
                      FROM samples
                      WHERE samples.tag_id = t.tag_id AND ts <= $2
                      ORDER BY ts DESC
                      LIMIT 1
                  ) s
                  """, conn);
        cmd.Parameters.Add(new NpgsqlParameter { Value = StoreUtil.IntTags(tagIds) });
        cmd.Parameters.Add(new NpgsqlParameter { Value = at.UtcDateTime });
        if (bounded)
        {
            cmd.Parameters.Add(new NpgsqlParameter { Value = at.UtcDateTime.AddHours(-3) });
        }

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await ScanSamples(reader, carried: false, ct);
    }

    public async Task UpsertTagsAsync(IReadOnlyList<Tag> tags, CancellationToken ct = default)
    {
        if (tags.Count == 0)
        {
            return;
        }

        await using var conn = await Open(ct);
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
        await using var conn = await Open(ct);
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

    public ValueTask DisposeAsync() => _ds?.DisposeAsync() ?? ValueTask.CompletedTask;

    private async Task<NpgsqlConnection> Open(CancellationToken ct)
    {
        if (_ds is null)
        {
            throw new InvalidOperationException(_initError ?? "timescaledb is not configured");
        }

        return await _ds.OpenConnectionAsync(ct);
    }

    private static async Task<IReadOnlyList<Sample>> ScanSamples(NpgsqlDataReader reader, bool? carried, CancellationToken ct)
    {
        var output = new List<Sample>(4096);
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
