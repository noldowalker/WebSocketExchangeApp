using Aggregator.Core.Models;

namespace Aggregator.Core.Persistence;

public interface ITradeTickSink
{
    Task WriteBatchAsync(IReadOnlyCollection<TradeTick> batch, CancellationToken cancellationToken);
}
