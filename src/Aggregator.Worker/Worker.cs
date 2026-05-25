using System.Net.WebSockets;
using System.Text;
using Aggregator.Core.Normalization;

namespace Aggregator.Worker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly ITickNormalizer _tickNormalizer;
    private readonly string _exchangeAUrl;

    public Worker(ILogger<Worker> logger, IConfiguration configuration, ITickNormalizer tickNormalizer)
    {
        _logger = logger;
        _tickNormalizer = tickNormalizer;
        _exchangeAUrl = configuration["Exchange:ExchangeAUrl"] ?? "ws://localhost:5121/ws/exchange-a";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var socket = new ClientWebSocket();
        await socket.ConnectAsync(new Uri(_exchangeAUrl), stoppingToken);
        _logger.LogInformation("Connected to {Url}", _exchangeAUrl);

        var buffer = new byte[4096];

        while (!stoppingToken.IsCancellationRequested && socket.State == WebSocketState.Open)
        {
            var builder = new StringBuilder();
            WebSocketReceiveResult result;

            do
            {
                result = await socket.ReceiveAsync(buffer, stoppingToken);

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing", stoppingToken);
                    _logger.LogInformation("WebSocket closed by server.");
                    return;
                }

                builder.Append(Encoding.UTF8.GetString(buffer, 0, result.Count));
            } while (!result.EndOfMessage);

            var rawPayload = builder.ToString();
            _logger.LogInformation("Raw tick: {Payload}", rawPayload);

            if (_tickNormalizer.TryNormalize(rawPayload, out var tick))
            {
                _logger.LogInformation(
                    "Normalized tick: Source={Source}, Value={Value}, TimestampUtc={TimestampUtc:O}",
                    tick!.Source,
                    tick.Value,
                    tick.TimestampUtc);
            }
            else
            {
                _logger.LogWarning("Failed to normalize tick payload: {Payload}", rawPayload);
            }
        }
    }
}
