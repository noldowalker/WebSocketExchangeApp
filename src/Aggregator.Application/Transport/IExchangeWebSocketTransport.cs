namespace Aggregator.Application.Transport;

public interface IExchangeWebSocketTransport : IAsyncDisposable
{
    Task ConnectAsync(Uri uri, CancellationToken cancellationToken);
    IAsyncEnumerable<string> ReadMessagesAsync(CancellationToken cancellationToken);
}
