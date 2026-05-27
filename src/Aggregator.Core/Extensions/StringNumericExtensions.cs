using System.Globalization;

namespace Aggregator.Core.Extensions;

public static class StringNumericExtensions
{
    public static int ToPositiveOrDefault(this string? value, int fallback)
    {
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) || parsed <= 0)
        {
            return fallback;
        }

        return parsed;
    }

    public static int ToNonNegativeOrDefault(this string? value, int fallback)
    {
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) || parsed < 0)
        {
            return fallback;
        }

        return parsed;
    }

    public static double ToRatioOrDefault(this string? value, double fallback)
    {
        if (!double.TryParse(value, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var parsed) ||
            parsed < 0d ||
            parsed > 1d)
        {
            return fallback;
        }

        return parsed;
    }
}
