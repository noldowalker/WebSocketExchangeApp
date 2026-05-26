using Aggregator.Core.Normalization;

namespace UnitTests.Core.Normalization;

public class KrakenTickNormalizerTests
{
    private readonly KrakenTickNormalizer _sut = new();

    [Test]
    public void TryNormalize_ValidPayload_ReturnsTrueAndParsedTick()
    {
        var payload = """
            {"name":"Kraken","channel":"ticker","type":"update","data":[{"symbol":"BTC/USD","last":68321.11,"volume":2.75,"timestamp":"2026-05-26T12:00:00.0000000+00:00"}]}
            """;

        var result = _sut.TryNormalize(payload, out var tick);

        Assert.That(result, Is.True);
        Assert.That(tick, Is.Not.Null);
        Assert.That(tick!.Ticker, Is.EqualTo("BTC/USD"));
        Assert.That(tick.Price, Is.EqualTo(68321.11m));
        Assert.That(tick.Volume, Is.EqualTo(2.75m));
    }

    [Test]
    public void TryNormalize_EmptyDataArray_ReturnsFalse()
    {
        var result = _sut.TryNormalize("""{"data":[]}""", out var tick);

        Assert.That(result, Is.False);
        Assert.That(tick, Is.Null);
    }

    [Test]
    public void TryNormalize_MissingSymbol_ReturnsFalse()
    {
        var payload = """
            {"data":[{"last":68321.11,"volume":2.75,"timestamp":"2026-05-26T12:00:00.0000000+00:00"}]}
            """;

        var result = _sut.TryNormalize(payload, out var tick);

        Assert.That(result, Is.False);
        Assert.That(tick, Is.Null);
    }

    [Test]
    public void TryNormalize_InvalidVolume_ReturnsFalse()
    {
        var payload = """
            {"data":[{"symbol":"BTC/USD","last":68321.11,"volume":"bad","timestamp":"2026-05-26T12:00:00.0000000+00:00"}]}
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
