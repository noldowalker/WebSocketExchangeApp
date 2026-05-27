using Aggregator.Core.Extensions;
using Aggregator.Application.Processing;
using Aggregator.Application.Models;

namespace Aggregator.Application.Configuration;

public static class WorkerConfigurationReader
{
    public static string GetRequiredPostgresConnectionString(IConfiguration configuration)
    {
        var connectionString = configuration.GetConnectionString("Postgres");
        if (!string.IsNullOrWhiteSpace(connectionString))
        {
            return connectionString;
        }

        throw CreateStartupException(
            "Configuration value 'ConnectionStrings:Postgres' is required. Aggregator.Application cannot start without a PostgreSQL connection.");
    }

    public static IReadOnlyList<ExchangeConnectionOptions> GetRequiredExchangeConnections(IConfiguration configuration)
    {
        var connections = configuration
            .GetSection("Exchange:Connections")
            .Get<List<ExchangeConnectionOptions>>();

        if (connections is null || connections.Count == 0)
        {
            throw CreateStartupException(
                "Configuration section 'Exchange:Connections' must contain at least one connection. Aggregator.Application cannot start without exchange sources.");
        }

        return connections
            .Select(NormalizeConnection)
            .ToArray();
    }

    public static BatchingOptions GetBatchingOptions(IConfiguration configuration)
    {
        return new BatchingOptions
        {
            BatchSize = configuration["Batching:BatchSize"].ToPositiveOrDefault(100),
            BatchTimeoutMs = configuration["Batching:BatchTimeoutMs"].ToPositiveOrDefault(1000)
        };
    }

    private static ExchangeConnectionOptions NormalizeConnection(ExchangeConnectionOptions connection)
    {
        if (string.IsNullOrWhiteSpace(connection.Url))
        {
            throw CreateStartupException(
                $"Exchange connection for source '{connection.Source}' must define a non-empty Url.");
        }

        return new ExchangeConnectionOptions
        {
            Url = connection.Url,
            Source = connection.Source,
            Reconnect = NormalizeReconnectOptions(connection.Reconnect, connection.Source)
        };
    }

    private static ReconnectOptions NormalizeReconnectOptions(
        ReconnectOptions? reconnect,
        ExchangeSource source)
    {
        if (reconnect is null)
        {
            throw CreateStartupException(
                $"Exchange connection for source '{source}' must define a Reconnect section.");
        }

        return new ReconnectOptions
        {
            MaxAttempts = Math.Max(0, reconnect.MaxAttempts),
            ConnectTimeoutMs = Math.Max(1, reconnect.ConnectTimeoutMs),
            DelayMs = Math.Max(1, reconnect.DelayMs),
            MaxDelayMs = Math.Max(Math.Max(1, reconnect.DelayMs), reconnect.MaxDelayMs),
            JitterRatio = Math.Clamp(reconnect.JitterRatio, 0d, 1d)
        };
    }

    private static InvalidOperationException CreateStartupException(string message)
    {
        Console.Error.WriteLine(message);
        return new InvalidOperationException(message);
    }
}
