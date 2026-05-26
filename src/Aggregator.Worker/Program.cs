using Aggregator.Core.Normalization;
using Aggregator.Core.Persistence;
using Aggregator.Infrastructure.Persistence;
using Aggregator.Worker.Connection;
using Aggregator.Worker.Configuration;
using Aggregator.Worker.Diagnostics;
using Aggregator.Worker.Normalization;
using Aggregator.Worker.Processing;
using Aggregator.Worker;
using Microsoft.EntityFrameworkCore;

var builder = Host.CreateApplicationBuilder(args);
var connectionString = builder.Configuration.GetConnectionString("Postgres")
                      ?? "Host=localhost;Port=5432;Database=aggregator;Username=postgres;Password=postgres";

builder.Services.AddDbContextFactory<AggregatorDbContext>(options =>
    options.UseNpgsql(connectionString));

builder.Services.AddSingleton<ITickNormalizer<Aggregator.Core.Models.BinanceTick>, BinanceTickNormalizer>();
builder.Services.AddSingleton<ITickNormalizer<Aggregator.Core.Models.CoinbaseTick>, CoinbaseTickNormalizer>();
builder.Services.AddSingleton<ITickNormalizer<Aggregator.Core.Models.KrakenTick>, KrakenTickNormalizer>();
builder.Services.AddSingleton<IExchangeTickNormalizerRouter, ExchangeTickNormalizerRouter>();
builder.Services.AddSingleton(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    var connections = configuration.GetSection("Exchange:Connections").Get<List<ExchangeConnectionOptions>>() ?? [];
    if (connections.Count > 0)
    {
        return connections;
    }

    return new List<ExchangeConnectionOptions>
    {
        new()
        {
            Url = configuration["Exchange:Url"]
                ?? configuration["Exchange:ExchangeAUrl"]
                ?? "ws://localhost:5000/ws/binance",
            Source = Aggregator.Core.Models.ExchangeSource.Binance
        }
    };
});
builder.Services.AddSingleton(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    return new ReconnectOptions
    {
        MaxAttempts = GetNonNegative(configuration["Reconnect:MaxAttempts"], 0),
        ConnectTimeoutMs = GetPositive(configuration["Reconnect:ConnectTimeoutMs"], 5000),
        DelayMs = GetPositive(configuration["Reconnect:DelayMs"], 3000),
        MaxDelayMs = GetPositive(configuration["Reconnect:MaxDelayMs"], 30000),
        JitterRatio = GetRatio(configuration["Reconnect:JitterRatio"], 0.2d)
    };
});
builder.Services.AddSingleton<IReconnectPolicy, ExponentialBackoffReconnectPolicy>();
builder.Services.AddSingleton<ProcessingStats>();
builder.Services.AddSingleton<ITradeTickSink, PostgresTradeTickSink>();
builder.Services.AddSingleton(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    return new BatchingOptions
    {
        BatchSize = GetPositive(configuration["Batching:BatchSize"], 100),
        BatchTimeoutMs = GetPositive(configuration["Batching:BatchTimeoutMs"], 1000)
    };
});
builder.Services.AddSingleton<BatchingTickProcessor>();
builder.Services.AddHostedService<DatabaseMigrationHostedService>();
builder.Services.AddHostedService<Worker>();
builder.Services.AddHostedService<StatsHttpServerService>();

var host = builder.Build();
host.Run();

static int GetPositive(string? value, int fallback)
{
    if (!int.TryParse(value, out var parsed) || parsed <= 0)
    {
        return fallback;
    }

    return parsed;
}

static int GetNonNegative(string? value, int fallback)
{
    if (!int.TryParse(value, out var parsed) || parsed < 0)
    {
        return fallback;
    }

    return parsed;
}

static double GetRatio(string? value, double fallback)
{
    if (!double.TryParse(value, out var parsed) || parsed < 0d || parsed > 1d)
    {
        return fallback;
    }

    return parsed;
}
