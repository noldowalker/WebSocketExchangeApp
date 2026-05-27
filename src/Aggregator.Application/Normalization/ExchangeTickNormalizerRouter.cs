using Aggregator.Core.Normalization;
using Aggregator.Domain.Models;
using Aggregator.Application.Models;

namespace Aggregator.Application.Normalization;

public sealed class ExchangeTickNormalizerRouter : IExchangeTickNormalizerRouter
{
    private readonly ITickNormalizer<BinanceTick> _binanceTickNormalizer;
    private readonly ITickNormalizer<CoinbaseTick> _coinbaseTickNormalizer;
    private readonly ITickNormalizer<KrakenTick> _krakenTickNormalizer;

    public ExchangeTickNormalizerRouter(
        ITickNormalizer<BinanceTick> binanceTickNormalizer,
        ITickNormalizer<CoinbaseTick> coinbaseTickNormalizer,
        ITickNormalizer<KrakenTick> krakenTickNormalizer)
    {
        _binanceTickNormalizer = binanceTickNormalizer;
        _coinbaseTickNormalizer = coinbaseTickNormalizer;
        _krakenTickNormalizer = krakenTickNormalizer;
    }

    public bool TryNormalize(ExchangeSource source, string rawPayload, out TradeTick? tick)
    {
        tick = null;

        switch (source)
        {
            case ExchangeSource.Binance:
                if (!_binanceTickNormalizer.TryNormalize(rawPayload, out var binanceTick))
                {
                    return false;
                }

                tick = new TradeTick(
                    Source: "binance",
                    Ticker: binanceTick!.Ticker,
                    Price: binanceTick.Price,
                    Volume: binanceTick.Volume,
                    TimestampUtc: binanceTick.EventTimeUtc);
                return true;

            case ExchangeSource.Coinbase:
                if (!_coinbaseTickNormalizer.TryNormalize(rawPayload, out var coinbaseTick))
                {
                    return false;
                }

                tick = new TradeTick(
                    Source: "coinbase",
                    Ticker: coinbaseTick!.Ticker,
                    Price: coinbaseTick.Price,
                    Volume: coinbaseTick.Volume,
                    TimestampUtc: coinbaseTick.EventTimeUtc);
                return true;

            case ExchangeSource.Kraken:
                if (!_krakenTickNormalizer.TryNormalize(rawPayload, out var krakenTick))
                {
                    return false;
                }

                tick = new TradeTick(
                    Source: "kraken",
                    Ticker: krakenTick!.Ticker,
                    Price: krakenTick.Price,
                    Volume: krakenTick.Volume,
                    TimestampUtc: krakenTick.EventTimeUtc);
                return true;

            default:
                return false;
        }
    }
}
