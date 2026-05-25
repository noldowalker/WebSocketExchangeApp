using System.Threading;

namespace Aggregator.Worker.Diagnostics;

public sealed class ProcessingStats
{
    private long _startedAtUtcTicks;
    private long _rawReceived;
    private long _channelRead;
    private long _normalizedOk;
    private long _normalizedFailed;
    private long _reconnectAttemptsTotal;
    private long _reconnectAttemptsCurrentCycle;
    private long _connectFailures;
    private long _lastReconnectDelayMs;

    public void MarkStarted()
    {
        var nowTicks = DateTimeOffset.UtcNow.UtcTicks;
        Interlocked.CompareExchange(ref _startedAtUtcTicks, nowTicks, 0);
    }

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

    public StatsSnapshot Snapshot()
    {
        var rawReceived = Interlocked.Read(ref _rawReceived);
        var channelRead = Interlocked.Read(ref _channelRead);
        var normalizedOk = Interlocked.Read(ref _normalizedOk);
        var normalizedFailed = Interlocked.Read(ref _normalizedFailed);
        var reconnectAttemptsTotal = Interlocked.Read(ref _reconnectAttemptsTotal);
        var reconnectAttemptsCurrentCycle = Interlocked.Read(ref _reconnectAttemptsCurrentCycle);
        var connectFailures = Interlocked.Read(ref _connectFailures);
        var lastReconnectDelayMs = Interlocked.Read(ref _lastReconnectDelayMs);
        var startedAtTicks = Interlocked.Read(ref _startedAtUtcTicks);
        var elapsedSeconds = 1d;
        if (startedAtTicks > 0)
        {
            var startedAt = new DateTimeOffset(startedAtTicks, TimeSpan.Zero);
            elapsedSeconds = Math.Max((DateTimeOffset.UtcNow - startedAt).TotalSeconds, 1d);
        }

        var readPerSecond = channelRead / elapsedSeconds;

        return new StatsSnapshot(
            RawReceived: rawReceived,
            ChannelRead: channelRead,
            NormalizedOk: normalizedOk,
            NormalizedFailed: normalizedFailed,
            ReadPerSecond: readPerSecond,
            ReconnectAttemptsTotal: reconnectAttemptsTotal,
            ReconnectAttemptsCurrentCycle: reconnectAttemptsCurrentCycle,
            ConnectFailures: connectFailures,
            LastReconnectDelayMs: lastReconnectDelayMs);
    }
}

public sealed record StatsSnapshot(
    long RawReceived,
    long ChannelRead,
    long NormalizedOk,
    long NormalizedFailed,
    double ReadPerSecond,
    long ReconnectAttemptsTotal,
    long ReconnectAttemptsCurrentCycle,
    long ConnectFailures,
    long LastReconnectDelayMs);
