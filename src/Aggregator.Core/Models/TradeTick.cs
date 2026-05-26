namespace Aggregator.Core.Models;

public sealed record TradeTick(
    string Source,
    string Ticker,
    decimal Price,
    decimal Volume,
    DateTimeOffset TimestampUtc);
