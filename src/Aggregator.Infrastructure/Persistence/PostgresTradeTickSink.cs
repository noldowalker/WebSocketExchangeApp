using Aggregator.Core.Models;
using Aggregator.Core.Persistence;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using System.Data;
using System.Text;

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
        var connection = (NpgsqlConnection)dbContext.Database.GetDbConnection();
        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken);
        }

        await using var command = connection.CreateCommand();
        command.CommandText = BuildInsertCommandText(batch.Count);

        var index = 0;
        foreach (var tick in batch)
        {
            command.Parameters.AddWithValue($"p{index}_source", tick.Source);
            command.Parameters.AddWithValue($"p{index}_ticker", tick.Ticker);
            command.Parameters.AddWithValue($"p{index}_price", tick.Price);
            command.Parameters.AddWithValue($"p{index}_volume", tick.Volume);
            command.Parameters.AddWithValue($"p{index}_timestamp_utc", tick.TimestampUtc);
            index++;
        }

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string BuildInsertCommandText(int batchSize)
    {
        var sql = new StringBuilder();
        sql.Append("insert into trade_ticks (source, ticker, price, volume, timestamp_utc) values ");

        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0)
            {
                sql.Append(", ");
            }

            sql.Append($"(@p{i}_source, @p{i}_ticker, @p{i}_price, @p{i}_volume, @p{i}_timestamp_utc)");
        }

        sql.Append(" on conflict (source, ticker, price, volume, timestamp_utc) do nothing;");
        return sql.ToString();
    }
}
