namespace Aggregator.Core.Models;

public abstract record BaseTick(ExchangeSource Source);

public enum ExchangeSource
{
    Undefined = 0,
    Binance = 1
}
