using Aggregator.Core.Models;
using Aggregator.Core.Persistence;

namespace Aggregator.Worker.Persistence;

public sealed class NoOpTradeTickSink : ITradeTickSink
{
    public Task WriteBatchAsync(IReadOnlyCollection<TradeTick> batch, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
