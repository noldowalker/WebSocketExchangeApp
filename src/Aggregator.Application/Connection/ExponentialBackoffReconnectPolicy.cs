using Aggregator.Application.Configuration;

namespace Aggregator.Application.Connection;

public sealed class ExponentialBackoffReconnectPolicy : IReconnectPolicy
{
    private readonly int _maxAttempts;
    private readonly int _baseDelayMs;
    private readonly int _maxDelayMs;
    private readonly double _jitterRatio;

    public ExponentialBackoffReconnectPolicy(ReconnectOptions options)
    {
        _maxAttempts = Math.Max(0, options.MaxAttempts);
        ConnectTimeoutMs = Math.Max(1, options.ConnectTimeoutMs);
        _baseDelayMs = Math.Max(1, options.DelayMs);
        _maxDelayMs = Math.Max(_baseDelayMs, options.MaxDelayMs);
        _jitterRatio = Math.Clamp(options.JitterRatio, 0d, 1d);
    }

    public int ConnectTimeoutMs { get; }

    public bool ShouldReconnect(long currentAttempts)
    {
        return _maxAttempts <= 0 || currentAttempts <= _maxAttempts;
    }

    public int CalculateDelayMs(long currentAttempts)
    {
        var exponentialFactor = Math.Max(0L, currentAttempts - 1L);
        var rawDelay = _baseDelayMs * Math.Pow(2d, Math.Min(exponentialFactor, 10L));
        var cappedDelay = Math.Min(rawDelay, _maxDelayMs);
        var jitterRange = cappedDelay * _jitterRatio;
        var jitter = (Random.Shared.NextDouble() * 2d - 1d) * jitterRange;
        var finalDelay = cappedDelay + jitter;
        return Math.Max(1, (int)Math.Round(finalDelay));
    }
}
