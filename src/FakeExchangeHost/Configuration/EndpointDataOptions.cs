namespace FakeExchangeHost.Configuration;

public sealed class EndpointDataOptions
{
    public string Name { get; init; } = "exchange-a";
    public int Port { get; init; } = 5000;
    public string Resource { get; init; } = "/ws/exchange-a";
    public int IntervalMs { get; init; } = 1000;
    public string PayloadJsonResourceName { get; init; } = "exchange-a.json";
}
