using Aggregator.Core.Persistence;
using Aggregator.Domain.Models;
using Aggregator.Application.Diagnostics;
using Aggregator.Application.Processing;
using Moq;

namespace UnitTests.Application.Processing;

public class BatchingTickProcessorTests
{
    [Test]
    public async Task AddAsync_WhenBatchSizeReached_FlushesToSink()
    {
        var sinkMock = new Mock<ITradeTickSink>();
        var processor = CreateProcessor(sinkMock.Object, batchSize: 2, batchTimeoutMs: 1000);

        await processor.AddAsync(CreateTick("BTCUSDT", 1), CancellationToken.None);
        await processor.AddAsync(CreateTick("ETHUSDT", 2), CancellationToken.None);

        sinkMock.Verify(
            x => x.WriteBatchAsync(
                It.Is<IReadOnlyCollection<TradeTick>>(batch => batch.Count == 2),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Test]
    public async Task AddAsync_WhenTimeoutReached_FlushesToSink()
    {
        var sinkMock = new Mock<ITradeTickSink>();
        var processor = CreateProcessor(sinkMock.Object, batchSize: 10, batchTimeoutMs: 1);

        await processor.AddAsync(CreateTick("BTCUSDT", 1), CancellationToken.None);
        await Task.Delay(10);
        await processor.AddAsync(CreateTick("ETHUSDT", 2), CancellationToken.None);

        sinkMock.Verify(
            x => x.WriteBatchAsync(
                It.Is<IReadOnlyCollection<TradeTick>>(batch => batch.Count >= 1),
                It.IsAny<CancellationToken>()),
            Times.AtLeastOnce);
    }

    [Test]
    public async Task AddAsync_DuplicateTickInSameBatch_IsWrittenOnce()
    {
        var sinkMock = new Mock<ITradeTickSink>();
        var processor = CreateProcessor(sinkMock.Object, batchSize: 2, batchTimeoutMs: 1000);
        var tick = CreateTick("BTCUSDT", 1);

        await processor.AddAsync(tick, CancellationToken.None);
        await processor.AddAsync(tick, CancellationToken.None);
        await processor.FlushAsync(CancellationToken.None);

        sinkMock.Verify(
            x => x.WriteBatchAsync(
                It.Is<IReadOnlyCollection<TradeTick>>(batch => batch.Count == 1 && batch.Single() == tick),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Test]
    public async Task FlushAsync_WithEmptyBuffer_DoesNotCallSink()
    {
        var sinkMock = new Mock<ITradeTickSink>();
        var processor = CreateProcessor(sinkMock.Object, batchSize: 2, batchTimeoutMs: 1000);

        await processor.FlushAsync(CancellationToken.None);

        sinkMock.Verify(x => x.WriteBatchAsync(It.IsAny<IReadOnlyCollection<TradeTick>>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Test]
    public async Task FlushAsync_ClearsBufferAndAcceptsNewTicks()
    {
        var sinkMock = new Mock<ITradeTickSink>();
        var processor = CreateProcessor(sinkMock.Object, batchSize: 10, batchTimeoutMs: 1000);

        await processor.AddAsync(CreateTick("BTCUSDT", 1), CancellationToken.None);
        await processor.FlushAsync(CancellationToken.None);
        await processor.AddAsync(CreateTick("ETHUSDT", 2), CancellationToken.None);
        await processor.FlushAsync(CancellationToken.None);

        sinkMock.Verify(x => x.WriteBatchAsync(It.IsAny<IReadOnlyCollection<TradeTick>>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    private static BatchingTickProcessor CreateProcessor(ITradeTickSink sink, int batchSize, int batchTimeoutMs)
    {
        return new BatchingTickProcessor(
            sink,
            new BatchingOptions
            {
                BatchSize = batchSize,
                BatchTimeoutMs = batchTimeoutMs
            },
            new ProcessingStats());
    }

    private static TradeTick CreateTick(string ticker, int secondsOffset)
    {
        return new TradeTick(
            Source: "binance",
            Ticker: ticker,
            Price: 100m,
            Volume: 1m,
            TimestampUtc: new DateTimeOffset(2026, 5, 26, 12, 0, secondsOffset, TimeSpan.Zero));
    }
}
