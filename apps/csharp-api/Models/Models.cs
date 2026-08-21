using System.Text.Json.Serialization;

namespace Prism.Api.Models;

public static class Contract
{
    public const string Version = "v1.2";
    public const ushort QualityGood = 192;
    public static readonly string[] Ops = ["write", "locf", "range", "tags"];
}

public sealed class Sample
{
    public DateTimeOffset Ts { get; set; }
    public uint TagId { get; set; }
    public double Value { get; set; }
    public ushort Quality { get; set; } = Contract.QualityGood;
    public bool Carried { get; set; }
}

public sealed class WriteSample
{
    public uint? Id { get; set; }

    [JsonPropertyName("tag_id")]
    public uint? TagId { get; set; }

    public DateTimeOffset? Date { get; set; }

    [JsonPropertyName("ts")]
    public DateTimeOffset? Ts { get; set; }

    public double Value { get; set; }
    public ushort? Quality { get; set; }

    public Sample Normalize(DateTimeOffset now)
    {
        var id = Id ?? TagId ?? 0;
        var ts = Date ?? Ts;
        if (ts is null || ts.Value == default)
        {
            ts = now;
        }

        return new Sample
        {
            Ts = ts.Value.ToUniversalTime(),
            TagId = id,
            Value = Value,
            Quality = Quality ?? Contract.QualityGood
        };
    }
}

public sealed class SamplesWrap
{
    public List<WriteSample> Samples { get; set; } = [];
}

public sealed class WriteResponse
{
    public int Written { get; set; }
}

public sealed class Tag
{
    public uint Id { get; set; }
    public string Name { get; set; } = "";
    public string? Unit { get; set; }
}

public sealed class TagList
{
    public List<Tag> Tags { get; set; } = [];
}

public sealed class TagWriteRequest
{
    public List<Tag> Tags { get; set; } = [];
}

public sealed class TagWriteResponse
{
    public int Upserted { get; set; }
}

public sealed class ValuesRequest
{
    public string? RequestKey { get; set; }
    public List<uint> TagsId { get; set; } = [];
    public DateTimeOffset? Exact { get; set; }
    public DateTimeOffset? Old { get; set; }
    public DateTimeOffset? Young { get; set; }

    public string Mode() =>
        Old is not null && Old != default && Young is not null && Young != default ? "range" : "locf";

    public DateTimeOffset At()
    {
        if (Exact is not null && Exact != default)
        {
            return Exact.Value.ToUniversalTime();
        }

        return DateTimeOffset.UtcNow;
    }
}

public sealed class ValueRecord
{
    public DateTimeOffset Date { get; set; }
    public double Value { get; set; }
    public ushort Quality { get; set; }
}

public sealed class ValuesTag
{
    public uint Id { get; set; }
    public List<ValueRecord> Values { get; set; } = [];
}

public sealed class ValuesResponse
{
    public string? RequestKey { get; set; }
    public List<ValuesTag> Tags { get; set; } = [];
}

public sealed class Meta
{
    public string Backend { get; set; } = "csharp";
    public string Storage { get; set; } = "";
    public string[] Storages { get; set; } = [];
    public string Contract { get; set; } = Models.Contract.Version;
    public string[] Ops { get; set; } = Models.Contract.Ops;
}

public sealed class ErrorBody
{
    public ErrorDetail Error { get; set; } = new();
}

public sealed class ErrorDetail
{
    public string Code { get; set; } = "";
    public string Message { get; set; } = "";
}
