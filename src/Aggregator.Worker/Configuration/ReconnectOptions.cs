namespace Aggregator.Worker.Configuration;

public sealed class ReconnectOptions
{
    public int MaxAttempts { get; init; } = 0;
    public int ConnectTimeoutMs { get; init; } = 5000;
    public int DelayMs { get; init; } = 3000;
    public int MaxDelayMs { get; init; } = 30000;
    public double JitterRatio { get; init; } = 0.2d;
}
