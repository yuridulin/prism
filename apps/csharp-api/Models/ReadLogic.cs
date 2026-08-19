using System.Globalization;

namespace Prism.Api.Models;

public static class ReadLogic
{
    public static bool ValidMode(string? mode) =>
        mode is "locf" or "range" or "sample" or "twavg";

    public static TimeSpan ParseStep(string? raw)
    {
        if (string.IsNullOrEmpty(raw))
        {
            return TimeSpan.FromMinutes(1);
        }

        try
        {
            var totalMs = 0d;
            var i = 0;
            while (i < raw.Length)
            {
                var start = i;
                if (raw[i] is '+' or '-')
                {
                    i++;
                }

                while (i < raw.Length && (char.IsDigit(raw[i]) || raw[i] == '.'))
                {
                    i++;
                }

                if (start == i || i == start + 1 && raw[start] is '+' or '-')
                {
                    return TimeSpan.FromMinutes(1);
                }

                var num = double.Parse(raw[start..i], CultureInfo.InvariantCulture);
                var unitStart = i;
                while (i < raw.Length && char.IsLetter(raw[i]))
                {
                    i++;
                }

                var unit = raw[unitStart..i];
                var ms = unit switch
                {
                    "ns" => num / 1_000_000d,
                    "us" or "µs" => num / 1_000d,
                    "ms" => num,
                    "s" => num * 1_000d,
                    "m" => num * 60_000d,
                    "h" => num * 3_600_000d,
                    _ => double.NaN
                };
                if (double.IsNaN(ms))
                {
                    return TimeSpan.FromMinutes(1);
                }

                totalMs += ms;
            }

            return totalMs > 0 ? TimeSpan.FromMilliseconds(totalMs) : TimeSpan.FromMinutes(1);
        }
        catch (FormatException)
        {
            return TimeSpan.FromMinutes(1);
        }
    }

    public static List<Series> GroupByTag(IReadOnlyList<uint> tagIds, IReadOnlyList<Sample> samples)
    {
        var index = new Dictionary<uint, int>();
        var output = new List<Series>(tagIds.Count);
        foreach (var id in tagIds)
        {
            if (index.ContainsKey(id))
            {
                continue;
            }

            index[id] = output.Count;
            output.Add(new Series { TagId = id, Samples = [] });
        }

        foreach (var sample in samples)
        {
            if (!index.TryGetValue(sample.TagId, out var i))
            {
                index[sample.TagId] = output.Count;
                output.Add(new Series { TagId = sample.TagId, Samples = [sample] });
                continue;
            }

            output[i].Samples.Add(sample);
        }

        foreach (var series in output)
        {
            series.Samples.Sort(static (a, b) => a.Ts.CompareTo(b.Ts));
        }

        return output;
    }

    public static List<Series> Resample(IReadOnlyList<Series> series, DateTimeOffset from, DateTimeOffset to, TimeSpan step)
    {
        if (step <= TimeSpan.Zero)
        {
            step = TimeSpan.FromMinutes(1);
        }

        var output = new List<Series>(series.Count);
        foreach (var src in series)
        {
            var dst = new Series { TagId = src.TagId, Samples = [] };
            for (var t = from; t < to || t == from; t += step)
            {
                if (t > to || t == to && t != from)
                {
                    break;
                }

                if (!LastAtOrBefore(src.Samples, t, out var last))
                {
                    continue;
                }

                dst.Samples.Add(new Sample
                {
                    Ts = t.ToUniversalTime(),
                    TagId = src.TagId,
                    Value = last.Value,
                    Quality = last.Quality,
                    Carried = last.Carried || last.Ts != t
                });
            }

            output.Add(dst);
        }

        return output;
    }

    public static List<Series> TimeWeightedAvg(IReadOnlyList<Series> series, DateTimeOffset from, DateTimeOffset to)
    {
        if (to <= from)
        {
            return [];
        }

        var output = new List<Series>(series.Count);
        foreach (var src in series)
        {
            var dst = new Series { TagId = src.TagId, Samples = [] };
            var weighted = 0d;
            var weight = 0d;
            var points = src.Samples;
            for (var j = 0; j < points.Count; j++)
            {
                var start = points[j].Ts;
                if (start < from)
                {
                    start = from;
                }

                var end = to;
                if (j + 1 < points.Count && points[j + 1].Ts < to)
                {
                    end = points[j + 1].Ts;
                }

                if (end <= start)
                {
                    continue;
                }

                var dt = (end - start).TotalSeconds;
                weighted += points[j].Value * dt;
                weight += dt;
            }

            if (weight > 0)
            {
                dst.Value = weighted / weight;
            }

            output.Add(dst);
        }

        return output;
    }

    public static ReadResult Assemble(string mode, ReadRequest req, IReadOnlyList<Sample> raw)
    {
        var series = GroupByTag(req.TagIds, raw);
        var result = new ReadResult { Mode = mode, Series = series };
        switch (mode)
        {
            case "locf":
                result.At = UtcOrNull(req.At);
                break;
            case "range":
                result.From = UtcOrNull(req.From);
                result.To = UtcOrNull(req.To);
                break;
            case "sample":
                var stepRaw = string.IsNullOrEmpty(req.Step) ? "1m" : req.Step;
                result.From = UtcOrNull(req.From);
                result.To = UtcOrNull(req.To);
                result.Step = stepRaw;
                result.Series = Resample(series, req.From!.Value.ToUniversalTime(), req.To!.Value.ToUniversalTime(), ParseStep(stepRaw));
                break;
            case "twavg":
                result.From = UtcOrNull(req.From);
                result.To = UtcOrNull(req.To);
                result.Series = TimeWeightedAvg(series, req.From!.Value.ToUniversalTime(), req.To!.Value.ToUniversalTime());
                break;
        }

        return result;
    }

    private static bool LastAtOrBefore(IReadOnlyList<Sample> samples, DateTimeOffset at, out Sample last)
    {
        last = null!;
        var found = false;
        foreach (var sample in samples)
        {
            if (sample.Ts > at)
            {
                continue;
            }

            if (!found || sample.Ts > last.Ts)
            {
                last = sample;
                found = true;
            }
        }

        return found;
    }

    private static DateTimeOffset? UtcOrNull(DateTimeOffset? value) =>
        value is null || value.Value == default ? null : value.Value.ToUniversalTime();
}
