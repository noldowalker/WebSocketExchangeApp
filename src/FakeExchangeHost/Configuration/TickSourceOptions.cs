namespace FakeExchangeHost.Configuration;

public sealed class TickSourceOptions
{
    public static TickSourceOptions Default { get; } = new()
    {
        Name = "default-socket-value",
        Port = 5000,
        Resource = "/ws/default-socket-value",
        IntervalMs = 1000,
        PayloadJson = "{\"value\":\"default-socket-value\"}"
    };

    public string Name { get; init; } = "default-socket-value";
    public int Port { get; init; } = 5000;
    public string Resource { get; init; } = "/ws/default-socket-value";
    public int IntervalMs { get; init; } = 1000;
    public string PayloadJson { get; init; } = "{\"value\":\"default-socket-value\"}";
}
