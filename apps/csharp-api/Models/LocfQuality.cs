namespace Prism.Api.Models;

/// <summary>
/// OPC DA bands plus Sinus locf variants. StaleAfter = 48h, same as Datalake.
/// </summary>
public static class LocfQuality
{
    public const ushort Bad = 0;
    public const ushort BadLocf = 100;
    public const ushort UncertainLastUsable = 64;
    public const ushort Good = 192;
    public const ushort GoodLocf = 200;

    public static readonly TimeSpan StaleAfter = TimeSpan.FromHours(48);

    public static bool IsGoodBand(ushort quality) => quality >= Good;

    public static ushort Carry(ushort original, DateTimeOffset observed, DateTimeOffset requested)
    {
        if (observed >= requested)
        {
            return original;
        }

        var lag = requested - observed;
        if (lag <= StaleAfter)
        {
            return IsGoodBand(original) ? GoodLocf : BadLocf;
        }

        return IsGoodBand(original) ? UncertainLastUsable : BadLocf;
    }

    public static Sample StampLocf(Sample sample, DateTimeOffset at)
    {
        var observed = sample.Ts;
        return new Sample
        {
            Ts = observed,
            TagId = sample.TagId,
            Value = sample.Value,
            Quality = Carry(sample.Quality, observed, at),
            Carried = observed < at
        };
    }

    public static IReadOnlyList<Sample> StampLocf(IReadOnlyList<Sample> rows, DateTimeOffset at)
    {
        if (rows.Count == 0)
        {
            return rows;
        }

        var output = new List<Sample>(rows.Count);
        foreach (var sample in rows)
        {
            output.Add(StampLocf(sample, at));
        }

        return output;
    }

    public static IReadOnlyList<Sample> StampRangeCarried(IReadOnlyList<Sample> rows, DateTimeOffset old)
    {
        if (rows.Count == 0)
        {
            return rows;
        }

        var output = new List<Sample>(rows.Count);
        foreach (var sample in rows)
        {
            if (!sample.Carried)
            {
                output.Add(sample);
                continue;
            }

            output.Add(StampLocf(sample, old));
        }

        return output;
    }
}
