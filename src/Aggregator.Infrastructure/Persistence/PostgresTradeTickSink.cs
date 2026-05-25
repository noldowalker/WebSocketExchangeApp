using Aggregator.Core.Models;
using Aggregator.Core.Persistence;
using Microsoft.EntityFrameworkCore;

namespace Aggregator.Infrastructure.Persistence;

public sealed class PostgresTradeTickSink : ITradeTickSink
{
    private readonly IDbContextFactory<AggregatorDbContext> _dbContextFactory;

    public PostgresTradeTickSink(IDbContextFactory<AggregatorDbContext> dbContextFactory)
    {
        _dbContextFactory = dbContextFactory;
    }

    public async Task WriteBatchAsync(IReadOnlyCollection<TradeTick> batch, CancellationToken cancellationToken)
    {
        if (batch.Count == 0)
        {
            return;
        }

        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);
        var entities = batch.Select(x => new TradeTickEntity
        {
            Source = x.Source,
            Value = x.Value,
            TimestampUtc = x.TimestampUtc
        });

        await dbContext.TradeTicks.AddRangeAsync(entities, cancellationToken);
        await dbContext.SaveChangesAsync(cancellationToken);
    }
}
