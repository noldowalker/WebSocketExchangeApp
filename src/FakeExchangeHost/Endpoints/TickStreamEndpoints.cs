using System.Net.WebSockets;
using System.Globalization;
using System.Text;
using FakeExchangeHost.Configuration;

namespace FakeExchangeHost.Endpoints;

public static class TickStreamEndpoints
{
    private const string PriceToken = "{{PRICE}}";
    private const string VolumeToken = "{{VOLUME}}";
    private const string TimestampMsToken = "{{TIMESTAMP_MS}}";
    private const string TimestampIsoToken = "{{TIMESTAMP_ISO}}";

    public static IEndpointRouteBuilder MapTickStreamEndpoint(
        this IEndpointRouteBuilder endpoints,
        TickSourceOptions source)
    {
        endpoints.Map(source.Resource, async context =>
        {
            if (!context.WebSockets.IsWebSocketRequest)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            using var socket = await context.WebSockets.AcceptWebSocketAsync();
            var template = source.PayloadJson;

            while (socket.State == WebSocketState.Open && !context.RequestAborted.IsCancellationRequested)
            {
                var price = 50000m + (decimal)Random.Shared.NextDouble() * 25000m;
                var volume = 0.05m + (decimal)Random.Shared.NextDouble() * 15m;
                var now = DateTimeOffset.UtcNow;
                var timestampMs = now.ToUnixTimeMilliseconds();

                var payload = RenderPayload(template, price, volume, timestampMs, now);
                var bytes = Encoding.UTF8.GetBytes(payload);

                await socket.SendAsync(
                    bytes,
                    WebSocketMessageType.Text,
                    endOfMessage: true,
                    cancellationToken: context.RequestAborted);

                await Task.Delay(TimeSpan.FromMilliseconds(source.IntervalMs), context.RequestAborted);
            }
        });

        return endpoints;
    }

    private static string RenderPayload(
        string template,
        decimal price,
        decimal volume,
        long timestampMs,
        DateTimeOffset timestampUtc)
    {
        Span<char> priceBuffer = stackalloc char[32];
        Span<char> volumeBuffer = stackalloc char[32];
        Span<char> timestampBuffer = stackalloc char[32];
        Span<char> timestampIsoBuffer = stackalloc char[64];

        if (!price.TryFormat(priceBuffer, out var priceCharsWritten, "0.00####", CultureInfo.InvariantCulture) ||
            !volume.TryFormat(volumeBuffer, out var volumeCharsWritten, "0.000###", CultureInfo.InvariantCulture) ||
            !timestampMs.TryFormat(timestampBuffer, out var timestampCharsWritten, provider: CultureInfo.InvariantCulture) ||
            !timestampUtc.TryFormat(timestampIsoBuffer, out var timestampIsoCharsWritten, "O", CultureInfo.InvariantCulture))
        {
            throw new InvalidOperationException("Failed to format dynamic tick values.");
        }

        var builder = new StringBuilder(template.Length + 32);
        var cursor = 0;

        while (cursor < template.Length)
        {
            var priceTokenIndex = template.IndexOf(PriceToken, cursor, StringComparison.Ordinal);
            var volumeTokenIndex = template.IndexOf(VolumeToken, cursor, StringComparison.Ordinal);
            var timestampTokenIndex = template.IndexOf(TimestampMsToken, cursor, StringComparison.Ordinal);
            var timestampIsoTokenIndex = template.IndexOf(TimestampIsoToken, cursor, StringComparison.Ordinal);

            var nextTokenIndex = MinPositive(priceTokenIndex, volumeTokenIndex, timestampTokenIndex, timestampIsoTokenIndex);
            if (nextTokenIndex < 0)
            {
                builder.Append(template.AsSpan(cursor));
                break;
            }

            builder.Append(template.AsSpan(cursor, nextTokenIndex - cursor));

            if (nextTokenIndex == priceTokenIndex)
            {
                builder.Append(priceBuffer[..priceCharsWritten]);
                cursor = nextTokenIndex + PriceToken.Length;
            }
            else if (nextTokenIndex == volumeTokenIndex)
            {
                builder.Append(volumeBuffer[..volumeCharsWritten]);
                cursor = nextTokenIndex + VolumeToken.Length;
            }
            else
            {
                if (nextTokenIndex == timestampIsoTokenIndex)
                {
                    builder.Append(timestampIsoBuffer[..timestampIsoCharsWritten]);
                    cursor = nextTokenIndex + TimestampIsoToken.Length;
                }
                else
                {
                    builder.Append(timestampBuffer[..timestampCharsWritten]);
                    cursor = nextTokenIndex + TimestampMsToken.Length;
                }
            }
        }

        return builder.ToString();
    }

    private static int MinPositive(int first, int second, int third, int fourth)
    {
        var min = int.MaxValue;

        if (first >= 0 && first < min)
        {
            min = first;
        }

        if (second >= 0 && second < min)
        {
            min = second;
        }

        if (third >= 0 && third < min)
        {
            min = third;
        }

        if (fourth >= 0 && fourth < min)
        {
            min = fourth;
        }

        return min == int.MaxValue ? -1 : min;
    }
}
