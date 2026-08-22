using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Prism.Api.Models;

public sealed class UtcRfc3339Converter : JsonConverter<DateTimeOffset>
{
    public override DateTimeOffset Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var text = reader.GetString();
        if (string.IsNullOrEmpty(text))
        {
            return default;
        }

        return DateTimeOffset.Parse(
            text,
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
    }

    public override void Write(Utf8JsonWriter writer, DateTimeOffset value, JsonSerializerOptions options) =>
        WriteUtc(writer, value);

    public static void WriteUtc(Utf8JsonWriter writer, DateTimeOffset value)
    {
        var utc = value.UtcDateTime;
        Span<byte> buf = stackalloc byte[20];
        WriteDigits(buf, 0, utc.Year, 4);
        buf[4] = (byte)'-';
        WriteDigits(buf, 5, utc.Month, 2);
        buf[7] = (byte)'-';
        WriteDigits(buf, 8, utc.Day, 2);
        buf[10] = (byte)'T';
        WriteDigits(buf, 11, utc.Hour, 2);
        buf[13] = (byte)':';
        WriteDigits(buf, 14, utc.Minute, 2);
        buf[16] = (byte)':';
        WriteDigits(buf, 17, utc.Second, 2);
        buf[19] = (byte)'Z';
        writer.WriteStringValue(buf);
    }

    private static void WriteDigits(Span<byte> buf, int offset, int value, int width)
    {
        for (var i = width - 1; i >= 0; i--)
        {
            buf[offset + i] = (byte)('0' + (value % 10));
            value /= 10;
        }
    }
}
