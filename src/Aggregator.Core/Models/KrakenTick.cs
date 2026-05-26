namespace Aggregator.Core.Models;

public sealed record KrakenTick(
    string Ticker,
    decimal Price,
    decimal Volume,
    DateTimeOffset EventTimeUtc) : BaseTick(ExchangeSource.Kraken);
