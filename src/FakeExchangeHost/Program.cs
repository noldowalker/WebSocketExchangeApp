using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.UseWebSockets();

app.Map("/ws/exchange-a", async context =>
{
    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = StatusCodes.Status400BadRequest;
        return;
    }

    using var socket = await context.WebSockets.AcceptWebSocketAsync();

    while (socket.State == WebSocketState.Open && !context.RequestAborted.IsCancellationRequested)
    {
        var payload = JsonSerializer.Serialize(new
        {
            value = Random.Shared.Next(1, 101)
        });

        var bytes = Encoding.UTF8.GetBytes(payload);
        await socket.SendAsync(
            bytes,
            WebSocketMessageType.Text,
            endOfMessage: true,
            cancellationToken: context.RequestAborted);

        await Task.Delay(TimeSpan.FromSeconds(1), context.RequestAborted);
    }
});

app.Run();
