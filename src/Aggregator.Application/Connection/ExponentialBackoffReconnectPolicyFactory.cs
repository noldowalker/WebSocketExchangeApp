using Aggregator.Application.Configuration;

namespace Aggregator.Application.Connection;

public sealed class ExponentialBackoffReconnectPolicyFactory : IReconnectPolicyFactory
{
    public IReconnectPolicy Create(ReconnectOptions options)
    {
        return new ExponentialBackoffReconnectPolicy(options);
    }
}
