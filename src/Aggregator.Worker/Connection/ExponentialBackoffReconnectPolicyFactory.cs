using Aggregator.Worker.Configuration;

namespace Aggregator.Worker.Connection;

public sealed class ExponentialBackoffReconnectPolicyFactory : IReconnectPolicyFactory
{
    public IReconnectPolicy Create(ReconnectOptions options)
    {
        return new ExponentialBackoffReconnectPolicy(options);
    }
}
