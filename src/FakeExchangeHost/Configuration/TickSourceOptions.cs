namespace FakeExchangeHost.Configuration;

public sealed class TickSourceOptions
{
    public static TickSourceOptions Default { get; } = new()
    {
        Name = "exchange-a",
        Port = 5000,
        Resource = "/ws/exchange-a",
        IntervalMs = 1000,
        PayloadJson = "{\"value\":42}"
    };

    public string Name { get; init; } = "exchange-a";
    public int Port { get; init; } = 5000;
    public string Resource { get; init; } = "/ws/exchange-a";
    public int IntervalMs { get; init; } = 1000;
    public string PayloadJson { get; init; } = "{\"value\":42}";
}
