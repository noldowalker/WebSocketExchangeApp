using System.Collections.Concurrent;
using System.Threading;
using Aggregator.Core.Models;

namespace Aggregator.Worker.Diagnostics;

public sealed class ProcessingStats
{
    private readonly ConcurrentDictionary<ExchangeSource, ConnectionStats> _connections = new();
    private long _startedAtUtcTicks;
    private long _rawReceived;
    private long _channelRead;
    private long _normalizedOk;
    private long _normalizedFailed;
    private long _reconnectAttemptsTotal;
    private long _connectFailures;
    private long _batchesFlushedTotal;
    private long _lastBatchSize;

    public void MarkStarted()
    {
        var nowTicks = DateTimeOffset.UtcNow.UtcTicks;
        Interlocked.CompareExchange(ref _startedAtUtcTicks, nowTicks, 0);
    }

    public void IncrementRawReceived(ExchangeSource source)
    {
        Interlocked.Increment(ref _rawReceived);
        GetConnection(source).IncrementRawReceived();
    }

    public void IncrementChannelRead(ExchangeSource source)
    {
        Interlocked.Increment(ref _channelRead);
        GetConnection(source).IncrementChannelRead();
    }

    public void IncrementNormalizedOk(ExchangeSource source)
    {
        Interlocked.Increment(ref _normalizedOk);
        GetConnection(source).IncrementNormalizedOk();
    }

    public void IncrementNormalizedFailed(ExchangeSource source)
    {
        Interlocked.Increment(ref _normalizedFailed);
        GetConnection(source).IncrementNormalizedFailed();
    }

    public void IncrementReconnectAttempt(ExchangeSource source)
    {
        Interlocked.Increment(ref _reconnectAttemptsTotal);
        GetConnection(source).IncrementReconnectAttempt();
    }

    public void IncrementConnectFailures(ExchangeSource source)
    {
        Interlocked.Increment(ref _connectFailures);
        GetConnection(source).IncrementConnectFailures();
    }

    public void SetLastReconnectDelayMs(ExchangeSource source, int delayMs)
    {
        GetConnection(source).SetLastReconnectDelayMs(delayMs);
    }

    public void ResetReconnectCycleAttempts(ExchangeSource source)
    {
        GetConnection(source).ResetReconnectCycleAttempts();
    }

    public long GetReconnectAttemptsCurrentCycle(ExchangeSource source)
    {
        return GetConnection(source).GetReconnectAttemptsCurrentCycle();
    }

    public void IncrementBatchesFlushed() => Interlocked.Increment(ref _batchesFlushedTotal);
    public void SetLastBatchSize(int batchSize) => Interlocked.Exchange(ref _lastBatchSize, batchSize);

    public StatsSnapshot Snapshot()
    {
        var startedAtTicks = Interlocked.Read(ref _startedAtUtcTicks);
        var elapsedSeconds = GetElapsedSeconds(startedAtTicks);
        var perConnection = _connections
            .OrderBy(x => x.Key.ToString())
            .ToDictionary(
                x => x.Key.ToString(),
                x => x.Value.Snapshot(elapsedSeconds));

        return new StatsSnapshot(
            RawReceived: Interlocked.Read(ref _rawReceived),
            ChannelRead: Interlocked.Read(ref _channelRead),
            NormalizedOk: Interlocked.Read(ref _normalizedOk),
            NormalizedFailed: Interlocked.Read(ref _normalizedFailed),
            ReadPerSecond: Interlocked.Read(ref _channelRead) / elapsedSeconds,
            ReconnectAttemptsTotal: Interlocked.Read(ref _reconnectAttemptsTotal),
            ConnectFailures: Interlocked.Read(ref _connectFailures),
            BatchesFlushedTotal: Interlocked.Read(ref _batchesFlushedTotal),
            LastBatchSize: Interlocked.Read(ref _lastBatchSize),
            Connections: perConnection);
    }

    private ConnectionStats GetConnection(ExchangeSource source)
    {
        return _connections.GetOrAdd(source, _ => new ConnectionStats());
    }

    private static double GetElapsedSeconds(long startedAtTicks)
    {
        if (startedAtTicks <= 0)
        {
            return 1d;
        }

        var startedAt = new DateTimeOffset(startedAtTicks, TimeSpan.Zero);
        return Math.Max((DateTimeOffset.UtcNow - startedAt).TotalSeconds, 1d);
    }

    private sealed class ConnectionStats
    {
        private long _rawReceived;
        private long _channelRead;
        private long _normalizedOk;
        private long _normalizedFailed;
        private long _reconnectAttemptsTotal;
        private long _reconnectAttemptsCurrentCycle;
        private long _connectFailures;
        private long _lastReconnectDelayMs;

        public void IncrementRawReceived() => Interlocked.Increment(ref _rawReceived);
        public void IncrementChannelRead() => Interlocked.Increment(ref _channelRead);
        public void IncrementNormalizedOk() => Interlocked.Increment(ref _normalizedOk);
        public void IncrementNormalizedFailed() => Interlocked.Increment(ref _normalizedFailed);
        public void IncrementReconnectAttempt()
        {
            Interlocked.Increment(ref _reconnectAttemptsTotal);
            Interlocked.Increment(ref _reconnectAttemptsCurrentCycle);
        }

        public void IncrementConnectFailures() => Interlocked.Increment(ref _connectFailures);
        public void SetLastReconnectDelayMs(int delayMs) => Interlocked.Exchange(ref _lastReconnectDelayMs, delayMs);
        public void ResetReconnectCycleAttempts() => Interlocked.Exchange(ref _reconnectAttemptsCurrentCycle, 0);
        public long GetReconnectAttemptsCurrentCycle() => Interlocked.Read(ref _reconnectAttemptsCurrentCycle);

        public ConnectionStatsSnapshot Snapshot(double elapsedSeconds)
        {
            var channelRead = Interlocked.Read(ref _channelRead);
            return new ConnectionStatsSnapshot(
                RawReceived: Interlocked.Read(ref _rawReceived),
                ChannelRead: channelRead,
                NormalizedOk: Interlocked.Read(ref _normalizedOk),
                NormalizedFailed: Interlocked.Read(ref _normalizedFailed),
                ReadPerSecond: channelRead / elapsedSeconds,
                ReconnectAttemptsTotal: Interlocked.Read(ref _reconnectAttemptsTotal),
                ReconnectAttemptsCurrentCycle: Interlocked.Read(ref _reconnectAttemptsCurrentCycle),
                ConnectFailures: Interlocked.Read(ref _connectFailures),
                LastReconnectDelayMs: Interlocked.Read(ref _lastReconnectDelayMs));
        }
    }
}

public sealed record StatsSnapshot(
    long RawReceived,
    long ChannelRead,
    long NormalizedOk,
    long NormalizedFailed,
    double ReadPerSecond,
    long ReconnectAttemptsTotal,
    long ConnectFailures,
    long BatchesFlushedTotal,
    long LastBatchSize,
    IReadOnlyDictionary<string, ConnectionStatsSnapshot> Connections);

public sealed record ConnectionStatsSnapshot(
    long RawReceived,
    long ChannelRead,
    long NormalizedOk,
    long NormalizedFailed,
    double ReadPerSecond,
    long ReconnectAttemptsTotal,
    long ReconnectAttemptsCurrentCycle,
    long ConnectFailures,
    long LastReconnectDelayMs);
