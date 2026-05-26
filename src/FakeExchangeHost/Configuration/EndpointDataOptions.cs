namespace FakeExchangeHost.Configuration;

public sealed class EndpointDataOptions
{
    public required string Name { get; init; }
    public required int Port { get; init; }
    public required string Resource { get; init; }
    public required int IntervalMs { get; init; }
    public required string PayloadJsonResourceName { get; init; }
}
