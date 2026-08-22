namespace Prism.Api.Models;

/// <summary>
/// In-memory locf grid. Fallback when the store has no native downsample.
/// </summary>
public static class SampleStretch
{
    public static List<Sample> Fill(
        IReadOnlyList<uint> tagIds,
        IReadOnlyList<Sample> raw,
        DateTimeOffset from,
        DateTimeOffset to,
        TimeSpan step)
    {
        from = from.ToUniversalTime();
        to = to.ToUniversalTime();
        if (step <= TimeSpan.Zero)
        {
            return [];
        }

        var last = new Dictionary<uint, Sample>(tagIds.Count);
        foreach (var sample in raw.OrderBy(s => s.Ts))
        {
            last[sample.TagId] = sample;
        }

        var output = new List<Sample>();
        for (var t = from; t <= to; t += step)
        {
            foreach (var id in tagIds)
            {
                if (!last.TryGetValue(id, out var obs) || obs.Ts > t)
                {
                    continue;
                }

                output.Add(new Sample
                {
                    Ts = t,
                    TagId = id,
                    Value = obs.Value,
                    Quality = LocfQuality.Carry(obs.Quality, obs.Ts, t),
                    Carried = obs.Ts < t
                });
            }
        }

        return output;
    }
}
