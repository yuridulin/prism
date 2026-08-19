using System.Text.Json.Serialization;

namespace Prism.Api.Models;

public static class Contract
{
    public const string Version = "v1.1";
    public const ushort QualityGood = 192;
    public static readonly string[] Ops = ["write", "locf", "range", "sample", "twavg", "tags"];
}

public sealed class Sample
{
    public DateTimeOffset Ts { get; set; }
    public uint TagId { get; set; }
    public double Value { get; set; }
    public ushort Quality { get; set; } = Contract.QualityGood;

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool Carried { get; set; }
}

public sealed class WriteSample
{
    public DateTimeOffset? Ts { get; set; }
    public uint TagId { get; set; }
    public double Value { get; set; }
    public ushort? Quality { get; set; }

    public Sample Normalize(DateTimeOffset now)
    {
        var ts = Ts is null || Ts.Value == default ? now : Ts.Value;
        return new Sample
        {
            Ts = ts.ToUniversalTime(),
            TagId = TagId,
            Value = Value,
            Quality = Quality ?? Contract.QualityGood
        };
    }
}

public sealed class WriteRequest
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

public sealed class ReadRequest
{
    public string Mode { get; set; } = "";
    public List<uint> TagIds { get; set; } = [];
    public DateTimeOffset? At { get; set; }
    public DateTimeOffset? From { get; set; }
    public DateTimeOffset? To { get; set; }
    public string? Step { get; set; }
}

public sealed class Series
{
    public uint TagId { get; set; }
    public double? Value { get; set; }
    public List<Sample> Samples { get; set; } = [];
}

public sealed class ReadResult
{
    public string Mode { get; set; } = "";
    public DateTimeOffset? At { get; set; }
    public DateTimeOffset? From { get; set; }
    public DateTimeOffset? To { get; set; }
    public string? Step { get; set; }
    public List<Series> Series { get; set; } = [];
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
