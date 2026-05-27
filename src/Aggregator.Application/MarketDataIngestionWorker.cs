using System.Threading.Channels;
using Aggregator.Domain.Models;
using Aggregator.Application.Connection;
using Aggregator.Application.Configuration;
using Aggregator.Application.Diagnostics;
using Aggregator.Application.Models;
using Aggregator.Application.Normalization;
using Aggregator.Application.Processing;
using Aggregator.Application.Transport;

namespace Aggregator.Application;

public class MarketDataIngestionWorker : BackgroundService
{
    private readonly ILogger<MarketDataIngestionWorker> _logger;
    private readonly IReadOnlyList<ExchangeConnectionOptions> _connections;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IReconnectPolicyFactory _reconnectPolicyFactory;
    private readonly IExchangeWebSocketTransportFactory _transportFactory;
    private readonly BatchingTickProcessor _batchingTickProcessor;
    private readonly ProcessingStats _processingStats;

    public MarketDataIngestionWorker(
        ILogger<MarketDataIngestionWorker> logger,
        IReadOnlyList<ExchangeConnectionOptions> connections,
        IServiceScopeFactory scopeFactory,
        IReconnectPolicyFactory reconnectPolicyFactory,
        IExchangeWebSocketTransportFactory transportFactory,
        BatchingTickProcessor batchingTickProcessor,
        ProcessingStats processingStats)
    {
        _logger = logger;
        _connections = connections;
        _scopeFactory = scopeFactory;
        _reconnectPolicyFactory = reconnectPolicyFactory;
        _transportFactory = transportFactory;
        _batchingTickProcessor = batchingTickProcessor;
        _processingStats = processingStats;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_connections.Count == 0)
        {
            _logger.LogWarning("No exchange connections configured.");
            return;
        }

        var normalizedTicksChannel = Channel.CreateBounded<TradeTick>(new BoundedChannelOptions(4096)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });

        var normalizedTicksConsumerTask = ProcessNormalizedTicksAsync(normalizedTicksChannel.Reader, stoppingToken);
        var connectionTasks = _connections
            .Select(connection => RunConnectionLoopAsync(connection, normalizedTicksChannel.Writer, stoppingToken))
            .ToArray();

        try
        {
            await Task.WhenAll(connectionTasks);
        }
        finally
        {
            normalizedTicksChannel.Writer.TryComplete();
            await normalizedTicksConsumerTask;
        }
    }

    private async Task RunConnectionLoopAsync(
        ExchangeConnectionOptions connection,
        ChannelWriter<TradeTick> normalizedTicksWriter,
        CancellationToken stoppingToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var tickNormalizerRouter = scope.ServiceProvider.GetRequiredService<IExchangeTickNormalizerRouter>();
        var reconnectPolicy = _reconnectPolicyFactory.Create(connection.Reconnect);

        while (!stoppingToken.IsCancellationRequested)
        {
            await using var transport = _transportFactory.Create();
            try
            {
                await ConnectWithTimeoutAsync(transport, new Uri(connection.Url), reconnectPolicy, stoppingToken);
                _processingStats.MarkStarted();
                _processingStats.ResetReconnectCycleAttempts(connection.Source);
                _logger.LogInformation("Connected to {Url} ({Source})", connection.Url, connection.Source);

                await ReadAndNormalizeRawDataAsync(
                    connection.Source,
                    transport,
                    tickNormalizerRouter,
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
                _processingStats.IncrementConnectFailures(connection.Source);
                _logger.LogWarning(ex, "WebSocket loop failed for {Source}. Will try to reconnect.", connection.Source);
            }

            if (!await WaitBeforeReconnectAsync(connection, reconnectPolicy, stoppingToken))
            {
                return;
            }
        }
    }

    private async Task ReadAndNormalizeRawDataAsync(
        ExchangeSource source,
        IExchangeWebSocketTransport transport,
        IExchangeTickNormalizerRouter tickNormalizerRouter,
        ChannelWriter<TradeTick> normalizedTicksWriter,
        CancellationToken cancellationToken)
    {
        await foreach (var rawPayload in transport.ReadMessagesAsync(cancellationToken))
        {
            _processingStats.IncrementRawReceived(source);
            _processingStats.IncrementChannelRead(source);

            if (tickNormalizerRouter.TryNormalize(source, rawPayload, out var tick))
            {
                _processingStats.IncrementNormalizedOk(source);
                await normalizedTicksWriter.WriteAsync(tick!, cancellationToken);
            }
            else
            {
                _processingStats.IncrementNormalizedFailed(source);
            }
        }
    }

    private async Task ProcessNormalizedTicksAsync(
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
            await _batchingTickProcessor.FlushAsync(CancellationToken.None);
        }
    }

    private async Task<bool> WaitBeforeReconnectAsync(
        ExchangeConnectionOptions connection,
        IReconnectPolicy reconnectPolicy,
        CancellationToken cancellationToken)
    {
        _processingStats.IncrementReconnectAttempt(connection.Source);

        var currentAttempts = _processingStats.GetReconnectAttemptsCurrentCycle(connection.Source);
        if (!reconnectPolicy.ShouldReconnect(currentAttempts))
        {
            _logger.LogError(
                "Reconnect attempts exceeded the configured maximum for {Source}. CurrentAttempts={CurrentAttempts}",
                connection.Source,
                currentAttempts);
            return false;
        }

        var delayMs = reconnectPolicy.CalculateDelayMs(currentAttempts);
        _processingStats.SetLastReconnectDelayMs(connection.Source, delayMs);

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
        IReconnectPolicy reconnectPolicy,
        CancellationToken cancellationToken)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(reconnectPolicy.ConnectTimeoutMs);
        await transport.ConnectAsync(uri, timeoutCts.Token);
    }
}
