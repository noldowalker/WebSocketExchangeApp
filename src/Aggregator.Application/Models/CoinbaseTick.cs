namespace Aggregator.Application.Models;

public sealed record CoinbaseTick(
    string Ticker,
    decimal Price,
    decimal Volume,
    DateTimeOffset EventTimeUtc) : BaseTick(ExchangeSource.Coinbase);
