namespace Aggregator.Application.Transport;

public sealed class ClientWebSocketTransportFactory : IExchangeWebSocketTransportFactory
{
    public IExchangeWebSocketTransport Create()
    {
        return new ClientWebSocketTransport();
    }
}
