namespace Aggregator.Worker.Processing;

public sealed class BatchingOptions
{
    public int BatchSize { get; init; } = 100;
    public int BatchTimeoutMs { get; init; } = 1000;
}
