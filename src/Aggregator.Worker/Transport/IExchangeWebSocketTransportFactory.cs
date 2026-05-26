namespace Aggregator.Worker.Transport;

public interface IExchangeWebSocketTransportFactory
{
    IExchangeWebSocketTransport Create();
}
