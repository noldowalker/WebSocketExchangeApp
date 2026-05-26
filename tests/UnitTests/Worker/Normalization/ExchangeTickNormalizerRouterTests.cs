using Aggregator.Core.Models;
using Aggregator.Core.Normalization;
using Aggregator.Worker.Normalization;
using Moq;

namespace UnitTests.Worker.Normalization;

public class ExchangeTickNormalizerRouterTests
{
    [Test]
    public void TryNormalize_Binance_UsesBinanceNormalizerOnly()
    {
        var binanceMock = new Mock<ITickNormalizer<BinanceTick>>();
        var coinbaseMock = new Mock<ITickNormalizer<CoinbaseTick>>();
        var krakenMock = new Mock<ITickNormalizer<KrakenTick>>();
        var expectedTick = new BinanceTick("BTCUSDT", 100m, 1m, DateTimeOffset.UtcNow);

        binanceMock.Setup(x => x.TryNormalize("payload", out expectedTick)).Returns(true);
        var sut = CreateRouter(binanceMock, coinbaseMock, krakenMock);

        var result = sut.TryNormalize(ExchangeSource.Binance, "payload", out var tradeTick);

        Assert.That(result, Is.True);
        Assert.That(tradeTick, Is.Not.Null);
        Assert.That(tradeTick!.Source, Is.EqualTo("binance"));
        Assert.That(tradeTick.Ticker, Is.EqualTo("BTCUSDT"));
        binanceMock.Verify(x => x.TryNormalize("payload", out expectedTick), Times.Once);
        coinbaseMock.VerifyNoOtherCalls();
        krakenMock.VerifyNoOtherCalls();
    }

    [Test]
    public void TryNormalize_Coinbase_UsesCoinbaseNormalizerOnly()
    {
        var binanceMock = new Mock<ITickNormalizer<BinanceTick>>();
        var coinbaseMock = new Mock<ITickNormalizer<CoinbaseTick>>();
        var krakenMock = new Mock<ITickNormalizer<KrakenTick>>();
        var expectedTick = new CoinbaseTick("BTC-USD", 101m, 2m, DateTimeOffset.UtcNow);

        coinbaseMock.Setup(x => x.TryNormalize("payload", out expectedTick)).Returns(true);
        var sut = CreateRouter(binanceMock, coinbaseMock, krakenMock);

        var result = sut.TryNormalize(ExchangeSource.Coinbase, "payload", out var tradeTick);

        Assert.That(result, Is.True);
        Assert.That(tradeTick, Is.Not.Null);
        Assert.That(tradeTick!.Source, Is.EqualTo("coinbase"));
        Assert.That(tradeTick.Ticker, Is.EqualTo("BTC-USD"));
        binanceMock.VerifyNoOtherCalls();
        coinbaseMock.Verify(x => x.TryNormalize("payload", out expectedTick), Times.Once);
        krakenMock.VerifyNoOtherCalls();
    }

    [Test]
    public void TryNormalize_Kraken_UsesKrakenNormalizerOnly()
    {
        var binanceMock = new Mock<ITickNormalizer<BinanceTick>>();
        var coinbaseMock = new Mock<ITickNormalizer<CoinbaseTick>>();
        var krakenMock = new Mock<ITickNormalizer<KrakenTick>>();
        var expectedTick = new KrakenTick("BTC/USD", 102m, 3m, DateTimeOffset.UtcNow);

        krakenMock.Setup(x => x.TryNormalize("payload", out expectedTick)).Returns(true);
        var sut = CreateRouter(binanceMock, coinbaseMock, krakenMock);

        var result = sut.TryNormalize(ExchangeSource.Kraken, "payload", out var tradeTick);

        Assert.That(result, Is.True);
        Assert.That(tradeTick, Is.Not.Null);
        Assert.That(tradeTick!.Source, Is.EqualTo("kraken"));
        Assert.That(tradeTick.Ticker, Is.EqualTo("BTC/USD"));
        binanceMock.VerifyNoOtherCalls();
        coinbaseMock.VerifyNoOtherCalls();
        krakenMock.Verify(x => x.TryNormalize("payload", out expectedTick), Times.Once);
    }

    [Test]
    public void TryNormalize_Undefined_ReturnsFalse()
    {
        var sut = CreateRouter(
            new Mock<ITickNormalizer<BinanceTick>>(),
            new Mock<ITickNormalizer<CoinbaseTick>>(),
            new Mock<ITickNormalizer<KrakenTick>>());

        var result = sut.TryNormalize(ExchangeSource.Undefined, "payload", out var tradeTick);

        Assert.That(result, Is.False);
        Assert.That(tradeTick, Is.Null);
    }

    [Test]
    public void TryNormalize_WhenInnerNormalizerSucceeds_MapsToTradeTick()
    {
        var binanceMock = new Mock<ITickNormalizer<BinanceTick>>();
        var coinbaseMock = new Mock<ITickNormalizer<CoinbaseTick>>();
        var krakenMock = new Mock<ITickNormalizer<KrakenTick>>();
        var eventTime = new DateTimeOffset(2026, 5, 26, 12, 0, 0, TimeSpan.Zero);
        var expectedTick = new BinanceTick("BTCUSDT", 123.45m, 6.7m, eventTime);

        binanceMock.Setup(x => x.TryNormalize("payload", out expectedTick)).Returns(true);
        var sut = CreateRouter(binanceMock, coinbaseMock, krakenMock);

        var result = sut.TryNormalize(ExchangeSource.Binance, "payload", out var tradeTick);

        Assert.That(result, Is.True);
        Assert.That(tradeTick, Is.EqualTo(new TradeTick("binance", "BTCUSDT", 123.45m, 6.7m, eventTime)));
    }

    private static ExchangeTickNormalizerRouter CreateRouter(
        Mock<ITickNormalizer<BinanceTick>> binanceMock,
        Mock<ITickNormalizer<CoinbaseTick>> coinbaseMock,
        Mock<ITickNormalizer<KrakenTick>> krakenMock)
    {
        return new ExchangeTickNormalizerRouter(
            binanceMock.Object,
            coinbaseMock.Object,
            krakenMock.Object);
    }
}
