namespace Aggregator.Infrastructure.Persistence;

public sealed class TradeTickEntity
{
    public long Id { get; set; }
    public string Source { get; set; } = string.Empty;
    public decimal Value { get; set; }
    public DateTimeOffset TimestampUtc { get; set; }
}
