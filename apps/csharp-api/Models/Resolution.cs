using System.Globalization;

namespace Prism.Api.Models;

public static class Resolution
{
    public static bool TryParse(string? raw, out TimeSpan step)
    {
        step = default;
        if (string.IsNullOrWhiteSpace(raw))
        {
            return false;
        }

        var text = raw.Trim().ToLowerInvariant();
        return text switch
        {
            "1s" or "s" or "second" => Ok(TimeSpan.FromSeconds(1), out step),
            "1m" or "m" or "minute" => Ok(TimeSpan.FromMinutes(1), out step),
            "1h" or "h" or "hour" => Ok(TimeSpan.FromHours(1), out step),
            "1d" or "d" or "day" => Ok(TimeSpan.FromDays(1), out step),
            _ => TryDuration(text, out step)
        };
    }

    public static string PgInterval(TimeSpan step)
    {
        if (step >= TimeSpan.FromDays(1) && step.Ticks % TimeSpan.TicksPerDay == 0)
        {
            return ((int)step.TotalDays).ToString(CultureInfo.InvariantCulture) + " days";
        }

        if (step >= TimeSpan.FromHours(1) && step.Ticks % TimeSpan.TicksPerHour == 0)
        {
            return ((int)step.TotalHours).ToString(CultureInfo.InvariantCulture) + " hours";
        }

        if (step >= TimeSpan.FromMinutes(1) && step.Ticks % TimeSpan.TicksPerMinute == 0)
        {
            return ((int)step.TotalMinutes).ToString(CultureInfo.InvariantCulture) + " minutes";
        }

        return Math.Max(1, (int)Math.Round(step.TotalSeconds)).ToString(CultureInfo.InvariantCulture) + " seconds";
    }

    private static bool Ok(TimeSpan value, out TimeSpan step)
    {
        step = value;
        return true;
    }

    private static bool TryDuration(string text, out TimeSpan step)
    {
        step = default;
        if (text.Length < 2)
        {
            return false;
        }

        var unit = text[^1];
        if (!double.TryParse(text[..^1], NumberStyles.Float, CultureInfo.InvariantCulture, out var n) || n <= 0)
        {
            return false;
        }

        step = unit switch
        {
            's' => TimeSpan.FromSeconds(n),
            'm' => TimeSpan.FromMinutes(n),
            'h' => TimeSpan.FromHours(n),
            'd' => TimeSpan.FromDays(n),
            _ => TimeSpan.Zero
        };
        return step > TimeSpan.Zero;
    }
}
