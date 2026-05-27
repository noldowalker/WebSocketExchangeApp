namespace Aggregator.Core.Normalization;

public interface ITickNormalizer<T> where T : class
{
    bool TryNormalize(string rawPayload, out T? tick);
}
