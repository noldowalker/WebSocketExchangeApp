using System.Text.Json;
using Aggregator.Core.Models;

namespace Aggregator.Core.Normalization;

public sealed class ExchangeATickNormalizer : ITickNormalizer
{
    public bool TryNormalize(string rawPayload, out TradeTick? tick)
    {
        tick = null;
        if (string.IsNullOrWhiteSpace(rawPayload))
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(rawPayload);
            if (!document.RootElement.TryGetProperty("value", out var valueElement))
            {
                return false;
            }

            if (!valueElement.TryGetDecimal(out var value))
            {
                return false;
            }

            tick = new TradeTick(
                Source: "exchange-a",
                Value: value,
                TimestampUtc: DateTimeOffset.UtcNow);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }
}
