using Aggregator.Domain.Models;
using Aggregator.Application.Models;

namespace Aggregator.Application.Normalization;

public interface IExchangeTickNormalizerRouter
{
    bool TryNormalize(ExchangeSource source, string rawPayload, out TradeTick? tick);
}
