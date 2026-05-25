namespace Aggregator.Core.Models;

public sealed record TradeTick(
    string Source,
    decimal Value,
    DateTimeOffset TimestampUtc);
