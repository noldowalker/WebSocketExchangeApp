using System.Net.WebSockets;
using System.Text;
using FakeExchangeHost.Configuration;

namespace FakeExchangeHost.Endpoints;

public static class TickStreamEndpoints
{
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
            var bytes = Encoding.UTF8.GetBytes(source.PayloadJson);

            while (socket.State == WebSocketState.Open && !context.RequestAborted.IsCancellationRequested)
            {
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
}
