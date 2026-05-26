using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;
using Aggregator.Core.Models;
using Aggregator.Worker.Configuration;
using Aggregator.Worker.Diagnostics;
using Aggregator.Worker.Normalization;
using Aggregator.Worker.Processing;

namespace Aggregator.Worker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IReadOnlyList<ExchangeConnectionOptions> _connections;
    private readonly IExchangeTickNormalizerRouter _tickNormalizerRouter;
    private readonly BatchingTickProcessor _batchingTickProcessor;
    private readonly ProcessingStats _stats;
    private readonly int _maxReconnectAttempts;
    private readonly int _reconnectDelayMs;

    public Worker(
        ILogger<Worker> logger,
        IConfiguration configuration,
        List<ExchangeConnectionOptions> connections,
        IExchangeTickNormalizerRouter tickNormalizerRouter,
        BatchingTickProcessor batchingTickProcessor,
        ProcessingStats stats)
    {
        _logger = logger;
        _connections = connections;
        _tickNormalizerRouter = tickNormalizerRouter;
        _batchingTickProcessor = batchingTickProcessor;
        _stats = stats;
        _maxReconnectAttempts = GetPositiveOrZero(configuration["Reconnect:MaxAttempts"], 0);
        _reconnectDelayMs = GetPositiveOrZero(configuration["Reconnect:DelayMs"], 1000);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_connections.Count == 0)
        {
            _logger.LogWarning("No exchange connections configured.");
            return;
        }

        var normalizedTicks = Channel.CreateBounded<TradeTick>(new BoundedChannelOptions(4096)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });

        var processorTask = ConsumeNormalizedTicksAsync(normalizedTicks.Reader, stoppingToken);
        var connectionTasks = _connections
            .Select(connection => RunConnectionLoopAsync(connection, normalizedTicks.Writer, stoppingToken))
            .ToArray();

        try
        {
            await Task.WhenAll(connectionTasks);
        }
        finally
        {
            normalizedTicks.Writer.TryComplete();
            await processorTask;
        }
    }

    private async Task RunConnectionLoopAsync(
        ExchangeConnectionOptions connection,
        ChannelWriter<TradeTick> normalizedTicksWriter,
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            using var socket = new ClientWebSocket();
            try
            {
                await socket.ConnectAsync(new Uri(connection.Url), stoppingToken);
                _stats.MarkStarted();
                _stats.ResetReconnectCycleAttempts();
                _logger.LogInformation("Connected to {Url} ({Source})", connection.Url, connection.Source);

                var channel = Channel.CreateBounded<string>(new BoundedChannelOptions(1024)
                {
                    SingleReader = true,
                    SingleWriter = true,
                    FullMode = BoundedChannelFullMode.Wait
                });

                var producer = ProduceRawTicksAsync(socket, channel.Writer, stoppingToken);
                var consumer = ConsumeRawTicksAsync(
                    connection.Source,
                    channel.Reader,
                    normalizedTicksWriter,
                    stoppingToken);
                await Task.WhenAll(producer, consumer);

                if (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _stats.IncrementConnectFailures();
                _logger.LogWarning(ex, "WebSocket loop failed for {Source}. Will try to reconnect.", connection.Source);
            }

            if (!await WaitBeforeReconnectAsync(connection, stoppingToken))
            {
                return;
            }
        }
    }

    private async Task ProduceRawTicksAsync(
        ClientWebSocket socket,
        ChannelWriter<string> writer,
        CancellationToken cancellationToken)
    {
        var buffer = new byte[4096];

        try
        {
            while (!cancellationToken.IsCancellationRequested && socket.State == WebSocketState.Open)
            {
                var builder = new StringBuilder();
                WebSocketReceiveResult result;

                do
                {
                    result = await socket.ReceiveAsync(buffer, cancellationToken);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing", cancellationToken);
                        _logger.LogInformation("WebSocket closed by server.");
                        return;
                    }

                    builder.Append(Encoding.UTF8.GetString(buffer, 0, result.Count));
                } while (!result.EndOfMessage);

                var rawPayload = builder.ToString();
                _stats.IncrementRawReceived();
                await writer.WriteAsync(rawPayload, cancellationToken);
            }
        }
        finally
        {
            writer.TryComplete();
        }
    }

    private async Task ConsumeRawTicksAsync(
        ExchangeSource source,
        ChannelReader<string> reader,
        ChannelWriter<TradeTick> normalizedTicksWriter,
        CancellationToken cancellationToken)
    {
        await foreach (var rawPayload in reader.ReadAllAsync(cancellationToken))
        {
            _stats.IncrementChannelRead();

            if (_tickNormalizerRouter.TryNormalize(source, rawPayload, out var tick))
            {
                _stats.IncrementNormalizedOk();
                await normalizedTicksWriter.WriteAsync(tick!, cancellationToken);
            }
            else
            {
                _stats.IncrementNormalizedFailed();
            }
        }
    }

    private async Task ConsumeNormalizedTicksAsync(
        ChannelReader<TradeTick> reader,
        CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var tick in reader.ReadAllAsync(cancellationToken))
            {
                await _batchingTickProcessor.AddAsync(tick, cancellationToken);
            }
        }
        finally
        {
            await _batchingTickProcessor.FlushAsync(cancellationToken);
        }
    }

    private async Task<bool> WaitBeforeReconnectAsync(
        ExchangeConnectionOptions connection,
        CancellationToken cancellationToken)
    {
        _stats.IncrementReconnectAttempt();
        _stats.SetLastReconnectDelayMs(_reconnectDelayMs);

        var currentAttempts = _stats.Snapshot().ReconnectAttemptsCurrentCycle;
        if (_maxReconnectAttempts > 0 && currentAttempts > _maxReconnectAttempts)
        {
            _logger.LogError(
                "Reconnect attempts exceeded the configured maximum. MaxAttempts={MaxAttempts}",
                _maxReconnectAttempts);
            return false;
        }

        _logger.LogInformation(
            "Reconnect attempt {Attempt} for {Source} in {DelayMs}ms.",
            currentAttempts,
            connection.Source,
            _reconnectDelayMs);

        await Task.Delay(_reconnectDelayMs, cancellationToken);
        return true;
    }

    private static int GetPositiveOrZero(string? value, int fallback)
    {
        if (!int.TryParse(value, out var parsed))
        {
            return fallback;
        }

        return parsed < 0 ? fallback : parsed;
    }
}
