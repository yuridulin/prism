using System.Text.Json.Serialization;

namespace Prism.Api.Models;

[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    PropertyNameCaseInsensitive = true,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    Converters = [typeof(UtcRfc3339Converter)])]
[JsonSerializable(typeof(WriteSample))]
[JsonSerializable(typeof(List<WriteSample>))]
[JsonSerializable(typeof(SamplesWrap))]
[JsonSerializable(typeof(WriteResponse))]
[JsonSerializable(typeof(ValuesRequest))]
[JsonSerializable(typeof(ValuesResponse))]
[JsonSerializable(typeof(ValuesTag))]
[JsonSerializable(typeof(ValueRecord))]
[JsonSerializable(typeof(Tag))]
[JsonSerializable(typeof(List<Tag>))]
[JsonSerializable(typeof(TagList))]
[JsonSerializable(typeof(TagWriteRequest))]
[JsonSerializable(typeof(TagWriteResponse))]
[JsonSerializable(typeof(Meta))]
[JsonSerializable(typeof(ErrorBody))]
[JsonSerializable(typeof(ErrorDetail))]
internal partial class PrismJsonContext : JsonSerializerContext;
