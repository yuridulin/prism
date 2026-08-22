using System.Text.Json;

namespace Prism.Api.Models;

public static class ReadLogic
{
    private static readonly JsonWriterOptions JsonOpts = new() { SkipValidation = true };

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

        return output;
    }

    public static ValuesResponse Assemble(ValuesRequest req, IReadOnlyList<Sample> raw) =>
        new()
        {
            RequestKey = string.IsNullOrEmpty(req.RequestKey) ? null : req.RequestKey,
            Tags = GroupByTag(req.TagsId, raw)
        };

    public static void WriteJson(Utf8JsonWriter writer, ValuesRequest req, IReadOnlyList<Sample> samples)
    {
        var index = new Dictionary<uint, int>(req.TagsId.Count);
        var ids = new List<uint>(req.TagsId.Count);
        var buckets = new List<List<Sample>>(req.TagsId.Count);
        foreach (var id in req.TagsId)
        {
            if (index.ContainsKey(id))
            {
                continue;
            }

            index[id] = buckets.Count;
            ids.Add(id);
            buckets.Add(new List<Sample>(64));
        }

        foreach (var sample in samples)
        {
            if (!index.TryGetValue(sample.TagId, out var i))
            {
                index[sample.TagId] = buckets.Count;
                ids.Add(sample.TagId);
                buckets.Add([sample]);
                continue;
            }

            buckets[i].Add(sample);
        }

        writer.WriteStartObject();
        if (!string.IsNullOrEmpty(req.RequestKey))
        {
            writer.WriteString("requestKey", req.RequestKey);
        }

        writer.WritePropertyName("tags");
        writer.WriteStartArray();
        for (var t = 0; t < ids.Count; t++)
        {
            writer.WriteStartObject();
            writer.WriteNumber("id", ids[t]);
            writer.WritePropertyName("values");
            writer.WriteStartArray();
            foreach (var sample in buckets[t])
            {
                writer.WriteStartObject();
                writer.WritePropertyName("date");
                UtcRfc3339Converter.WriteUtc(writer, sample.Ts);
                writer.WriteNumber("value", sample.Value);
                writer.WriteNumber("quality", sample.Quality);
                writer.WriteEndObject();
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        writer.WriteEndArray();
        writer.WriteEndObject();
    }

    public sealed class ValuesJsonResult(ValuesRequest req, IReadOnlyList<Sample> samples) : IResult
    {
        public async Task ExecuteAsync(HttpContext http)
        {
            var buffer = new System.Buffers.ArrayBufferWriter<byte>(64 * 1024);
            using (var writer = new Utf8JsonWriter(buffer, JsonOpts))
            {
                WriteJson(writer, req, samples);
            }

            http.Response.ContentType = "application/json; charset=utf-8";
            http.Response.ContentLength = buffer.WrittenCount;
            await http.Response.Body.WriteAsync(buffer.WrittenMemory, http.RequestAborted);
        }
    }
}
