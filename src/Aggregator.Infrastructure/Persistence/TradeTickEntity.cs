namespace Aggregator.Infrastructure.Persistence;

public sealed class TradeTickEntity
{
    public long Id { get; set; }
    public string Source { get; set; } = string.Empty;
    public string Ticker { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public decimal Volume { get; set; }
    public DateTimeOffset TimestampUtc { get; set; }
}
