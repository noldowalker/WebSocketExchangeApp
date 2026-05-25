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
    public int Port { get; private init; } = 5000;
    public string Resource { get; private init; } = "/ws/exchange-a";
    public int IntervalMs { get; private init; } = 1000;
    public string PayloadJson { get; private init; } = "{\"value\":42}";
}
