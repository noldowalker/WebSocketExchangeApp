using System.Threading;

namespace Aggregator.Worker.Diagnostics;

public sealed class ProcessingStats
{
    private long _rawReceived;
    private long _normalizedOk;
    private long _normalizedFailed;

    public void IncrementRawReceived() => Interlocked.Increment(ref _rawReceived);
    public void IncrementNormalizedOk() => Interlocked.Increment(ref _normalizedOk);
    public void IncrementNormalizedFailed() => Interlocked.Increment(ref _normalizedFailed);

    public StatsSnapshot Snapshot() => new(
        RawReceived: Interlocked.Read(ref _rawReceived),
        NormalizedOk: Interlocked.Read(ref _normalizedOk),
        NormalizedFailed: Interlocked.Read(ref _normalizedFailed));
}

public sealed record StatsSnapshot(long RawReceived, long NormalizedOk, long NormalizedFailed);
