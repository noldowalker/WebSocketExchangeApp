using System.Globalization;
using System.Text.Json;
using Aggregator.Core.Models;

namespace Aggregator.Core.Normalization;

public sealed class KrakenTickNormalizer : ITickNormalizer<KrakenTick>
{
    public bool TryNormalize(string rawPayload, out KrakenTick? tick)
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

            if (!root.TryGetProperty("data", out var data) ||
                data.ValueKind != JsonValueKind.Array ||
                data.GetArrayLength() == 0)
            {
                return false;
            }

            var item = data[0];
            if (!TryReadString(item, "symbol", out var ticker) ||
                !TryReadDecimal(item, "last", out var price) ||
                !TryReadDecimal(item, "volume", out var volume) ||
                !TryReadTimestamp(item, "timestamp", out var eventTimeUtc))
            {
                return false;
            }

            tick = new KrakenTick(ticker!, price, volume, eventTimeUtc);
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
