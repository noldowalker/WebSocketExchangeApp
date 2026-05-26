using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;
using Aggregator.Core.Models;
using Aggregator.Core.Normalization;
using Aggregator.Worker.Diagnostics;
using Aggregator.Worker.Normalization;
using Aggregator.Worker.Processing;

namespace Aggregator.Worker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly ITickNormalizer<BinanceTick> _binanceTickNormalizer;
    private readonly IExchangeSourceResolver _exchangeSourceResolver;
    private readonly BatchingTickProcessor _batchingTickProcessor;
    private readonly ProcessingStats _stats;
    private readonly string _exchangeUrl;
    private readonly int _maxReconnectAttempts;
    private readonly int _reconnectDelayMs;

    public Worker(
        ILogger<Worker> logger,
        IConfiguration configuration,
        ITickNormalizer<BinanceTick> binanceTickNormalizer,
        IExchangeSourceResolver exchangeSourceResolver,
        BatchingTickProcessor batchingTickProcessor,
        ProcessingStats stats)
    {
        _logger = logger;
        _binanceTickNormalizer = binanceTickNormalizer;
        _exchangeSourceResolver = exchangeSourceResolver;
        _batchingTickProcessor = batchingTickProcessor;
        _stats = stats;
        _exchangeUrl = configuration["Exchange:Url"]
            ?? configuration["Exchange:ExchangeAUrl"]
            ?? "ws://localhost:5000/ws/binance";
        _maxReconnectAttempts = GetPositiveOrZero(configuration["Reconnect:MaxAttempts"], 0);
        _reconnectDelayMs = GetPositiveOrZero(configuration["Reconnect:DelayMs"], 1000);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            using var socket = new ClientWebSocket();
            try
            {
                await socket.ConnectAsync(new Uri(_exchangeUrl), stoppingToken);
                _stats.MarkStarted();
                _stats.ResetReconnectCycleAttempts();
                _logger.LogInformation("Connected to {Url}", _exchangeUrl);

                var channel = Channel.CreateBounded<string>(new BoundedChannelOptions(1024)
                {
                    SingleReader = true,
                    SingleWriter = true,
                    FullMode = BoundedChannelFullMode.Wait
                });

                var producer = ProduceRawTicksAsync(socket, channel.Writer, stoppingToken);
                var consumer = ConsumeRawTicksAsync(channel.Reader, stoppingToken);
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
                _logger.LogWarning(ex, "WebSocket loop failed. Will try to reconnect.");
            }

            if (!await WaitBeforeReconnectAsync(stoppingToken))
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

    private async Task ConsumeRawTicksAsync(ChannelReader<string> reader, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var rawPayload in reader.ReadAllAsync(cancellationToken))
            {
                _stats.IncrementChannelRead();

                var source = _exchangeSourceResolver.Resolve(rawPayload);
                if (source == ExchangeSource.Binance &&
                    _binanceTickNormalizer.TryNormalize(rawPayload, out var binanceTick))
                {
                    var tick = new TradeTick(
                        Source: "binance",
                        Ticker: binanceTick!.Ticker,
                        Price: binanceTick.Price,
                        Volume: binanceTick.Volume,
                        TimestampUtc: binanceTick.EventTimeUtc);
                    _stats.IncrementNormalizedOk();
                    await _batchingTickProcessor.AddAsync(tick, cancellationToken);
                }
                else
                {
                    _stats.IncrementNormalizedFailed();
                }
            }
        }
        finally
        {
            await _batchingTickProcessor.FlushAsync(cancellationToken);
        }
    }

    private async Task<bool> WaitBeforeReconnectAsync(CancellationToken cancellationToken)
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
            "Reconnect attempt {Attempt} in {DelayMs}ms.",
            currentAttempts,
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
