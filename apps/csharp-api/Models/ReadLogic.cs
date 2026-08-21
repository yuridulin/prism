namespace Prism.Api.Models;

public static class ReadLogic
{
    public static List<ValuesTag> GroupByTag(IReadOnlyList<uint> tagIds, IReadOnlyList<Sample> samples)
    {
        var index = new Dictionary<uint, int>();
        var output = new List<ValuesTag>(tagIds.Count);
        foreach (var id in tagIds)
        {
            if (index.ContainsKey(id))
            {
                continue;
            }

            index[id] = output.Count;
            output.Add(new ValuesTag { Id = id, Values = [] });
        }

        foreach (var sample in samples)
        {
            var rec = new ValueRecord
            {
                Date = sample.Ts,
                Value = sample.Value,
                Quality = sample.Quality
            };
            if (!index.TryGetValue(sample.TagId, out var i))
            {
                index[sample.TagId] = output.Count;
                output.Add(new ValuesTag { Id = sample.TagId, Values = [rec] });
                continue;
            }

            output[i].Values.Add(rec);
        }

        foreach (var tag in output)
        {
            tag.Values.Sort(static (a, b) => a.Date.CompareTo(b.Date));
        }

        return output;
    }

    public static ValuesResponse Assemble(ValuesRequest req, IReadOnlyList<Sample> raw) =>
        new()
        {
            RequestKey = string.IsNullOrEmpty(req.RequestKey) ? null : req.RequestKey,
            Tags = GroupByTag(req.TagsId, raw)
        };
}
