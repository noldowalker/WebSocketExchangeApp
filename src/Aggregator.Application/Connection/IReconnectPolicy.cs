namespace Aggregator.Application.Connection;

public interface IReconnectPolicy
{
    int ConnectTimeoutMs { get; }
    bool ShouldReconnect(long currentAttempts);
    int CalculateDelayMs(long currentAttempts);
}
