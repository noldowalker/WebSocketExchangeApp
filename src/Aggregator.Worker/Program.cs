using Aggregator.Core.Normalization;
using Aggregator.Core.Persistence;
using Aggregator.Worker.Diagnostics;
using Aggregator.Worker.Persistence;
using Aggregator.Worker.Processing;
using Aggregator.Worker;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton<ITickNormalizer, ExchangeATickNormalizer>();
builder.Services.AddSingleton<ProcessingStats>();
builder.Services.AddSingleton<ITradeTickSink, NoOpTradeTickSink>();
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
