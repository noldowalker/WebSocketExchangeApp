using Aggregator.Core.Normalization;
using Aggregator.Worker.Diagnostics;
using Aggregator.Worker;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton<ITickNormalizer, ExchangeATickNormalizer>();
builder.Services.AddSingleton<ProcessingStats>();
builder.Services.AddHostedService<Worker>();
builder.Services.AddHostedService<StatsHttpServerService>();

var host = builder.Build();
host.Run();
