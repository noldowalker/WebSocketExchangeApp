namespace Aggregator.Worker.Transport;

public sealed class ClientWebSocketTransportFactory : IExchangeWebSocketTransportFactory
{
    public IExchangeWebSocketTransport Create()
    {
        return new ClientWebSocketTransport();
    }
}
