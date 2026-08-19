using System.Globalization;
using System.Text;

namespace Prism.Api.Store;

internal static class StoreUtil
{
    public static string JoinIds(IReadOnlyList<uint> ids)
    {
        var sb = new StringBuilder();
        for (var i = 0; i < ids.Count; i++)
        {
            if (i > 0)
            {
                sb.Append(',');
            }

            sb.Append(ids[i].ToString(CultureInfo.InvariantCulture));
        }

        return sb.ToString();
    }

    public static int[] IntTags(IReadOnlyList<uint> ids)
    {
        var output = new int[ids.Count];
        for (var i = 0; i < ids.Count; i++)
        {
            output[i] = unchecked((int)ids[i]);
        }

        return output;
    }

    public static string FormatFloat(double value)
    {
        var raw = value.ToString("G17", CultureInfo.InvariantCulture);
        if (raw.Contains('.') || raw.Contains('e') || raw.Contains('E'))
        {
            return raw;
        }

        return raw + ".0";
    }

    public static long UnixNano(DateTimeOffset ts)
    {
        return (ts.UtcDateTime.Ticks - DateTime.UnixEpoch.Ticks) * 100;
    }

    public static string Rfc3339Nano(DateTimeOffset ts)
    {
        return ts.ToUniversalTime().UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", CultureInfo.InvariantCulture);
    }

    public static string QuestDbTime(DateTimeOffset ts)
    {
        return ts.ToUniversalTime().UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture);
    }

    public static async Task EnsureSuccess(HttpResponseMessage response, string name, CancellationToken ct)
    {
        if ((int)response.StatusCode < 300)
        {
            return;
        }

        var body = await response.Content.ReadAsStringAsync(ct);
        throw new InvalidOperationException($"{name} status {(int)response.StatusCode}: {body}");
    }
}
