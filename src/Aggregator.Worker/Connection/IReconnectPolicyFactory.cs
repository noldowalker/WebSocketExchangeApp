using Aggregator.Worker.Configuration;

namespace Aggregator.Worker.Connection;

public interface IReconnectPolicyFactory
{
    IReconnectPolicy Create(ReconnectOptions options);
}
