using Aggregator.Application.Configuration;

namespace Aggregator.Application.Connection;

public interface IReconnectPolicyFactory
{
    IReconnectPolicy Create(ReconnectOptions options);
}
