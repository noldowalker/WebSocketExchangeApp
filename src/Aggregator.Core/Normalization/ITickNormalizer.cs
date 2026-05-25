using Aggregator.Core.Models;

namespace Aggregator.Core.Normalization;

public interface ITickNormalizer
{
    bool TryNormalize(string rawPayload, out TradeTick? tick);
}
