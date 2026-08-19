using System.Buffers;
using System.Buffers.Text;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Prism.Api.Store;

internal static class StoreUtil
{
    public static HttpClient CreatePooledHttp(TimeSpan timeout, string? baseAddress = null)
    {
        var handler = new SocketsHttpHandler
        {
            AutomaticDecompression = DecompressionMethods.None,
            PooledConnectionLifetime = TimeSpan.FromMinutes(10),
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
            MaxConnectionsPerServer = 32,
            EnableMultipleHttp2Connections = true,
            ConnectTimeout = TimeSpan.FromSeconds(5)
        };
        var http = new HttpClient(handler, disposeHandler: true)
        {
            Timeout = timeout,
            DefaultRequestVersion = HttpVersion.Version11,
            DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrLower
        };
        if (!string.IsNullOrEmpty(baseAddress))
        {
            http.BaseAddress = new Uri(baseAddress.EndsWith('/') ? baseAddress : baseAddress + "/");
        }

        return http;
    }

    public static (string Host, int Port) ParseHostPort(string raw, int defaultPort)
    {
        var value = raw.Trim();
        if (value.StartsWith("tcp://", StringComparison.OrdinalIgnoreCase))
        {
            value = value[6..];
        }
        else if (value.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
        {
            value = value[7..];
        }
        else if (value.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
        {
            value = value[8..];
        }

        var slash = value.IndexOf('/');
        if (slash >= 0)
        {
            value = value[..slash];
        }

        var colon = value.LastIndexOf(':');
        if (colon > 0 && int.TryParse(value[(colon + 1)..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var port))
        {
            return (value[..colon], port);
        }

        return (value, defaultPort);
    }

    public static bool IsTransient(Exception ex) =>
        ex is HttpRequestException or IOException or SocketException or TaskCanceledException or TimeoutException;
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

internal sealed class ByteWriter : IDisposable
{
    private byte[] _buf;
    private int _pos;
    private bool _returned;

    public ByteWriter(int capacity)
    {
        _buf = ArrayPool<byte>.Shared.Rent(Math.Max(capacity, 256));
    }

    public int Length => _pos;

    public byte[] Buffer => _buf;

    public ReadOnlyMemory<byte> Written => _buf.AsMemory(0, _pos);

    public void Dispose()
    {
        if (_returned)
        {
            return;
        }

        _returned = true;
        ArrayPool<byte>.Shared.Return(_buf);
        _buf = [];
    }

    public void AppendByte(byte value)
    {
        Ensure(1);
        _buf[_pos++] = value;
    }

    public void AppendAscii(ReadOnlySpan<char> text)
    {
        Ensure(text.Length);
        for (var i = 0; i < text.Length; i++)
        {
            _buf[_pos++] = (byte)text[i];
        }
    }

    public void AppendUtf8(ReadOnlySpan<char> text)
    {
        var max = Encoding.UTF8.GetMaxByteCount(text.Length);
        Ensure(max);
        _pos += Encoding.UTF8.GetBytes(text, _buf.AsSpan(_pos));
    }

    public void AppendUInt(uint value)
    {
        Ensure(11);
        if (Utf8Formatter.TryFormat(value, _buf.AsSpan(_pos), out var written))
        {
            _pos += written;
        }
    }

    public void AppendUShort(ushort value)
    {
        Ensure(6);
        if (Utf8Formatter.TryFormat(value, _buf.AsSpan(_pos), out var written))
        {
            _pos += written;
        }
    }

    public void AppendLong(long value)
    {
        Ensure(21);
        if (Utf8Formatter.TryFormat(value, _buf.AsSpan(_pos), out var written))
        {
            _pos += written;
        }
    }

    public void AppendIlpFloat(double value)
    {
        Span<char> tmp = stackalloc char[32];
        if (!value.TryFormat(tmp, out var n, "G17", CultureInfo.InvariantCulture))
        {
            AppendAscii(StoreUtil.FormatFloat(value));
            return;
        }

        var slice = tmp[..n];
        AppendAscii(slice);
        for (var i = 0; i < n; i++)
        {
            var c = slice[i];
            if (c is '.' or 'e' or 'E')
            {
                return;
            }
        }

        AppendAscii(".0");
    }

    private void Ensure(int extra)
    {
        if (_pos + extra <= _buf.Length)
        {
            return;
        }

        var next = Math.Max(_buf.Length * 2, _pos + extra);
        var grown = ArrayPool<byte>.Shared.Rent(next);
        _buf.AsSpan(0, _pos).CopyTo(grown);
        ArrayPool<byte>.Shared.Return(_buf);
        _buf = grown;
    }
}
