using Aggregator.Core.Normalization;
using Aggregator.Core.Persistence;
using Aggregator.Domain.Models;
using Aggregator.Infrastructure.Persistence;
using Aggregator.Application;
using Aggregator.Application.Configuration;
using Aggregator.Application.Connection;
using Aggregator.Application.Diagnostics;
using Aggregator.Application.Models;
using Aggregator.Application.Normalization;
using Aggregator.Application.Processing;
using Aggregator.Application.Transport;
using Microsoft.EntityFrameworkCore;

var builder = Host.CreateApplicationBuilder(args);
var configuration = builder.Configuration;

var connectionString = WorkerConfigurationReader.GetRequiredPostgresConnectionString(configuration);
var exchangeConnections = WorkerConfigurationReader.GetRequiredExchangeConnections(configuration);
var batchingOptions = WorkerConfigurationReader.GetBatchingOptions(configuration);

builder.Services.AddDbContextFactory<AggregatorDbContext>(options =>
    options.UseNpgsql(connectionString));

builder.Services.AddScoped<ITickNormalizer<BinanceTick>, BinanceTickNormalizer>();
builder.Services.AddScoped<ITickNormalizer<CoinbaseTick>, CoinbaseTickNormalizer>();
builder.Services.AddScoped<ITickNormalizer<KrakenTick>, KrakenTickNormalizer>();
builder.Services.AddScoped<IExchangeTickNormalizerRouter, ExchangeTickNormalizerRouter>();

builder.Services.AddSingleton(exchangeConnections);
builder.Services.AddSingleton(batchingOptions);
builder.Services.AddSingleton<IReconnectPolicyFactory, ExponentialBackoffReconnectPolicyFactory>();
builder.Services.AddSingleton<IExchangeWebSocketTransportFactory, ClientWebSocketTransportFactory>();
builder.Services.AddSingleton<ProcessingStats>();
builder.Services.AddSingleton<ITradeTickSink, PostgresTradeTickSink>();
builder.Services.AddSingleton<BatchingTickProcessor>();
builder.Services.AddHostedService<DatabaseMigrationHostedService>();
builder.Services.AddHostedService<MarketDataIngestionWorker>();
builder.Services.AddHostedService<StatsHttpServerService>();

var host = builder.Build();
host.Run();
