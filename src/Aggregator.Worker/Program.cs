using Aggregator.Core.Normalization;
using Aggregator.Core.Persistence;
using Aggregator.Infrastructure.Persistence;
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
builder.Services.AddSingleton<IExchangeSourceResolver, JsonExchangeSourceResolver>();
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
