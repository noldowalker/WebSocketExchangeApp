using System.Globalization;
using System.Text.Json;
using Aggregator.Core.Normalization;
using Aggregator.Application.Models;

namespace Aggregator.Application.Normalization;

public sealed class CoinbaseTickNormalizer : ITickNormalizer<CoinbaseTick>
{
    public bool TryNormalize(string rawPayload, out CoinbaseTick? tick)
    {
        tick = null;
        if (string.IsNullOrWhiteSpace(rawPayload))
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(rawPayload);
            var root = document.RootElement;

            if (!TryReadString(root, "product_id", out var ticker) ||
                !TryReadDecimal(root, "price", out var price) ||
                !TryReadDecimal(root, "last_size", out var volume) ||
                !TryReadTimestamp(root, "time", out var eventTimeUtc))
            {
                return false;
            }

            tick = new CoinbaseTick(ticker!, price, volume, eventTimeUtc);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
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

    private static bool TryReadDecimal(JsonElement element, string propertyName, out decimal value)
    {
        value = default;
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return false;
        }

        if (property.ValueKind == JsonValueKind.Number)
        {
            return property.TryGetDecimal(out value);
        }

        var text = property.GetString();
        return decimal.TryParse(text, NumberStyles.Number, CultureInfo.InvariantCulture, out value);
    }

    private static bool TryReadTimestamp(JsonElement element, string propertyName, out DateTimeOffset value)
    {
        value = default;
        if (!element.TryGetProperty(propertyName, out var property))
        {
            return false;
        }

        var text = property.GetString();
        return DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out value);
    }
}
