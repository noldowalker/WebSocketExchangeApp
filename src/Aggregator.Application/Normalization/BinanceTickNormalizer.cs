using System.Globalization;
using System.Text.Json;
using Aggregator.Core.Normalization;
using Aggregator.Application.Models;

namespace Aggregator.Application.Normalization;

public sealed class BinanceTickNormalizer : ITickNormalizer<BinanceTick>
{
    public bool TryNormalize(string rawPayload, out BinanceTick? tick)
    {
        tick = null;
        if (string.IsNullOrWhiteSpace(rawPayload))
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(rawPayload);
            if (!document.RootElement.TryGetProperty("data", out var data))
            {
                return false;
            }

            if (!TryReadDecimalString(data, "c", out var price) ||
                !TryReadDecimalString(data, "v", out var volume))
            {
                return false;
            }

            if (!TryReadString(data, "s", out var ticker))
            {
                return false;
            }

            if (!TryReadTimestampMs(data, "E", out var eventTimeUtc))
            {
                return false;
            }

            tick = new BinanceTick(ticker!, price, volume, eventTimeUtc);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static bool TryReadDecimalString(JsonElement element, string propertyName, out decimal value)
    {
        value = default;
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return false;
        }

        var text = property.GetString();
        return decimal.TryParse(text, NumberStyles.Number, CultureInfo.InvariantCulture, out value);
    }

    private static bool TryReadTimestampMs(JsonElement element, string propertyName, out DateTimeOffset value)
    {
        value = default;
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return false;
        }

        string? text = property.ValueKind switch
        {
            JsonValueKind.String => property.GetString(),
            JsonValueKind.Number => property.GetRawText(),
            _ => null
        };

        if (!long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ms))
        {
            return false;
        }

        value = DateTimeOffset.FromUnixTimeMilliseconds(ms);
        return true;
    }

    private static bool TryReadString(JsonElement element, string propertyName, out string? value)
    {
        value = null;
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return false;
        }

        value = property.GetString();
        return !string.IsNullOrWhiteSpace(value);
    }
}
