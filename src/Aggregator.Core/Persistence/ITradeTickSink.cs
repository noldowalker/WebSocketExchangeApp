using Aggregator.Domain.Models;

namespace Aggregator.Core.Persistence;

public interface ITradeTickSink
{
    Task WriteBatchAsync(IReadOnlyCollection<TradeTick> batch, CancellationToken cancellationToken);
}
