namespace Aggregator.Application.Models;

public sealed record BinanceTick(
    string Ticker,
    decimal Price,
    decimal Volume,
    DateTimeOffset EventTimeUtc) : BaseTick(ExchangeSource.Binance);
