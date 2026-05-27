using Aggregator.Core.Normalization;
using Aggregator.Application.Normalization;

namespace UnitTests.Core.Normalization;

public class BinanceTickNormalizerTests
{
    private readonly BinanceTickNormalizer _sut = new();

    [Test]
    public void TryNormalize_ValidPayload_ReturnsTrueAndParsedTick()
    {
        var payload = """
            {"name":"Binance","stream":"btcusdt@ticker","data":{"e":"24hrTicker","E":"1716710400123","s":"BTCUSDT","c":"68500.12","v":"4.567"}}
            """;

        var result = _sut.TryNormalize(payload, out var tick);

        Assert.That(result, Is.True);
        Assert.That(tick, Is.Not.Null);
        Assert.That(tick!.Ticker, Is.EqualTo("BTCUSDT"));
        Assert.That(tick.Price, Is.EqualTo(68500.12m));
        Assert.That(tick.Volume, Is.EqualTo(4.567m));
        Assert.That(tick.EventTimeUtc, Is.EqualTo(DateTimeOffset.FromUnixTimeMilliseconds(1716710400123)));
    }

    [Test]
    public void TryNormalize_MissingData_ReturnsFalse()
    {
        var result = _sut.TryNormalize("""{"name":"Binance"}""", out var tick);

        Assert.That(result, Is.False);
        Assert.That(tick, Is.Null);
    }

    [Test]
    public void TryNormalize_MissingPrice_ReturnsFalse()
    {
        var payload = """
            {"data":{"E":"1716710400123","s":"BTCUSDT","v":"4.567"}}
            """;

        var result = _sut.TryNormalize(payload, out var tick);

        Assert.That(result, Is.False);
        Assert.That(tick, Is.Null);
    }

    [Test]
    public void TryNormalize_InvalidTimestamp_ReturnsFalse()
    {
        var payload = """
            {"data":{"E":"bad","s":"BTCUSDT","c":"68500.12","v":"4.567"}}
            """;

        var result = _sut.TryNormalize(payload, out var tick);

        Assert.That(result, Is.False);
        Assert.That(tick, Is.Null);
    }

    [Test]
    public void TryNormalize_InvalidJson_ReturnsFalse()
    {
        var result = _sut.TryNormalize("""{"data":""", out var tick);

        Assert.That(result, Is.False);
        Assert.That(tick, Is.Null);
    }
}
