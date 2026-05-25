using Aggregator.Core.Normalization;
using Aggregator.Worker;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton<ITickNormalizer, ExchangeATickNormalizer>();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
