using System.Text.Json;
using Aggregator.Core.Models;

namespace Aggregator.Worker.Normalization;

public sealed class JsonExchangeSourceResolver : IExchangeSourceResolver
{
    public ExchangeSource Resolve(string rawPayload)
    {
        if (string.IsNullOrWhiteSpace(rawPayload))
        {
            return ExchangeSource.Undefined;
        }

        try
        {
            using var document = JsonDocument.Parse(rawPayload);
            var root = document.RootElement;
            var name = TryGetName(root);
            if (string.IsNullOrWhiteSpace(name))
            {
                return ExchangeSource.Undefined;
            }

            return Enum.TryParse<ExchangeSource>(name, ignoreCase: true, out var source)
                ? source
                : ExchangeSource.Undefined;
        }
        catch (JsonException)
        {
            return ExchangeSource.Undefined;
        }
    }

    private static string? TryGetName(JsonElement root)
    {
        foreach (var property in root.EnumerateObject())
        {
            if (!property.Name.Equals("name", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            return property.Value.ValueKind == JsonValueKind.String
                ? property.Value.GetString()
                : null;
        }

        return null;
    }
}
