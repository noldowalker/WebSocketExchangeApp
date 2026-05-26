using System.Threading.Channels;
using Aggregator.Core.Models;
using Aggregator.Worker.Connection;
using Aggregator.Worker.Configuration;
using Aggregator.Worker.Diagnostics;
using Aggregator.Worker.Normalization;
using Aggregator.Worker.Processing;
using Aggregator.Worker.Transport;

namespace Aggregator.Worker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IReadOnlyList<ExchangeConnectionOptions> _connections;
    private readonly IReconnectPolicy _reconnectPolicy;
    private readonly IExchangeWebSocketTransportFactory _transportFactory;
    private readonly IExchangeTickNormalizerRouter _tickNormalizerRouter;
    private readonly BatchingTickProcessor _batchingTickProcessor;
    private readonly ProcessingStats _stats;

    public Worker(
        ILogger<Worker> logger,
        List<ExchangeConnectionOptions> connections,
        IReconnectPolicy reconnectPolicy,
        IExchangeWebSocketTransportFactory transportFactory,
        IExchangeTickNormalizerRouter tickNormalizerRouter,
        BatchingTickProcessor batchingTickProcessor,
        ProcessingStats stats)
    {
        _logger = logger;
        _connections = connections;
        _reconnectPolicy = reconnectPolicy;
        _transportFactory = transportFactory;
        _tickNormalizerRouter = tickNormalizerRouter;
        _batchingTickProcessor = batchingTickProcessor;
        _stats = stats;
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
            await using var transport = _transportFactory.Create();
            try
            {
                await ConnectWithTimeoutAsync(transport, new Uri(connection.Url), stoppingToken);
                _stats.MarkStarted();
                _stats.ResetReconnectCycleAttempts(connection.Source);
                _logger.LogInformation("Connected to {Url} ({Source})", connection.Url, connection.Source);

                await ConsumeTransportAsync(
                    connection.Source,
                    transport,
                    normalizedTicksWriter,
                    stoppingToken);

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
                _stats.IncrementConnectFailures(connection.Source);
                _logger.LogWarning(ex, "WebSocket loop failed for {Source}. Will try to reconnect.", connection.Source);
            }

            if (!await WaitBeforeReconnectAsync(connection, stoppingToken))
            {
                return;
            }
        }
    }

    private async Task ConsumeTransportAsync(
        ExchangeSource source,
        IExchangeWebSocketTransport transport,
        ChannelWriter<TradeTick> normalizedTicksWriter,
        CancellationToken cancellationToken)
    {
        await foreach (var rawPayload in transport.ReadMessagesAsync(cancellationToken))
        {
            _stats.IncrementRawReceived(source);
            _stats.IncrementChannelRead(source);

            if (_tickNormalizerRouter.TryNormalize(source, rawPayload, out var tick))
            {
                _stats.IncrementNormalizedOk(source);
                await normalizedTicksWriter.WriteAsync(tick!, cancellationToken);
            }
            else
            {
                _stats.IncrementNormalizedFailed(source);
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
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Normal shutdown.
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
        _stats.IncrementReconnectAttempt(connection.Source);

        var currentAttempts = _stats.GetReconnectAttemptsCurrentCycle(connection.Source);
        if (!_reconnectPolicy.ShouldReconnect(currentAttempts))
        {
            _logger.LogError(
                "Reconnect attempts exceeded the configured maximum for {Source}. CurrentAttempts={CurrentAttempts}",
                connection.Source,
                currentAttempts);
            return false;
        }

        var delayMs = _reconnectPolicy.CalculateDelayMs(currentAttempts);
        _stats.SetLastReconnectDelayMs(connection.Source, delayMs);

        _logger.LogInformation(
            "Reconnect attempt {Attempt} for {Source} in {DelayMs}ms.",
            currentAttempts,
            connection.Source,
            delayMs);

        await Task.Delay(delayMs, cancellationToken);
        return true;
    }

    private async Task ConnectWithTimeoutAsync(
        IExchangeWebSocketTransport transport,
        Uri uri,
        CancellationToken cancellationToken)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(_reconnectPolicy.ConnectTimeoutMs);
        await transport.ConnectAsync(uri, timeoutCts.Token);
    }
}
