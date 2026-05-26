using Aggregator.Core.Models;

namespace Aggregator.Worker.Normalization;

public interface IExchangeTickNormalizerRouter
{
    bool TryNormalize(ExchangeSource source, string rawPayload, out TradeTick? tick);
}
