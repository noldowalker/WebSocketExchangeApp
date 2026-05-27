namespace Aggregator.Application.Models;

public abstract record BaseTick(ExchangeSource Source);

public enum ExchangeSource
{
    Undefined = 0,
    Binance = 1,
    Coinbase = 2,
    Kraken = 3
}
