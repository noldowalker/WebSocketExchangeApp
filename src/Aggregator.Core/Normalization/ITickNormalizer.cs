namespace Aggregator.Core.Normalization;

public interface ITickNormalizer<T> where T : Models.BaseTick
{
    bool TryNormalize(string rawPayload, out T? tick);
}
