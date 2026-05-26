namespace Aggregator.Worker.Connection;

public interface IReconnectPolicy
{
    int ConnectTimeoutMs { get; }
    bool ShouldReconnect(long currentAttempts);
    int CalculateDelayMs(long currentAttempts);
}
