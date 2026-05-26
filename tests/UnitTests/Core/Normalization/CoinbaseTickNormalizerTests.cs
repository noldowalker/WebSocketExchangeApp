using Aggregator.Core.Normalization;

namespace UnitTests.Core.Normalization;

public class CoinbaseTickNormalizerTests
{
    private readonly CoinbaseTickNormalizer _sut = new();

    [Test]
    public void TryNormalize_ValidPayloadWithNumericFields_ReturnsTrueAndParsedTick()
    {
        var payload = """
            {"name":"Coinbase","type":"ticker","product_id":"BTC-USD","price":68499.45,"last_size":1.25,"time":"2026-05-26T12:00:00.0000000+00:00"}
            """;

        var result = _sut.TryNormalize(payload, out var tick);

        Assert.That(result, Is.True);
        Assert.That(tick, Is.Not.Null);
        Assert.That(tick!.Ticker, Is.EqualTo("BTC-USD"));
        Assert.That(tick.Price, Is.EqualTo(68499.45m));
        Assert.That(tick.Volume, Is.EqualTo(1.25m));
    }

    [Test]
    public void TryNormalize_ValidPayloadWithStringNumericFields_ReturnsTrueAndParsedTick()
    {
        var payload = """
            {"product_id":"BTC-USD","price":"68499.45","last_size":"1.25","time":"2026-05-26T12:00:00.0000000+00:00"}
            """;

        var result = _sut.TryNormalize(payload, out var tick);

        Assert.That(result, Is.True);
        Assert.That(tick, Is.Not.Null);
        Assert.That(tick!.Price, Is.EqualTo(68499.45m));
        Assert.That(tick.Volume, Is.EqualTo(1.25m));
    }

    [Test]
    public void TryNormalize_MissingProductId_ReturnsFalse()
    {
        var payload = """
            {"price":68499.45,"last_size":1.25,"time":"2026-05-26T12:00:00.0000000+00:00"}
            """;

        var result = _sut.TryNormalize(payload, out var tick);

        Assert.That(result, Is.False);
        Assert.That(tick, Is.Null);
    }

    [Test]
    public void TryNormalize_InvalidTime_ReturnsFalse()
    {
        var payload = """
            {"product_id":"BTC-USD","price":68499.45,"last_size":1.25,"time":"bad"}
            """;

        var result = _sut.TryNormalize(payload, out var tick);

        Assert.That(result, Is.False);
        Assert.That(tick, Is.Null);
    }

    [Test]
    public void TryNormalize_InvalidJson_ReturnsFalse()
    {
        var result = _sut.TryNormalize("""{"product_id":""", out var tick);

        Assert.That(result, Is.False);
        Assert.That(tick, Is.Null);
    }
}
