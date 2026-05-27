using Aggregator.Core.Models;

namespace Aggregator.Worker.Configuration;

public sealed class ExchangeConnectionOptions
{
    public required string Url { get; init; }
    public required ExchangeSource Source { get; init; }
    public required ReconnectOptions Reconnect { get; init; }
}
