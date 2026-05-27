namespace Aggregator.Application.Transport;

public interface IExchangeWebSocketTransportFactory
{
    IExchangeWebSocketTransport Create();
}
