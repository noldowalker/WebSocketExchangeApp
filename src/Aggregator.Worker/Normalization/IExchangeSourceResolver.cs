using Aggregator.Core.Models;

namespace Aggregator.Worker.Normalization;

public interface IExchangeSourceResolver
{
    ExchangeSource Resolve(string rawPayload);
}
