using Aggregator.Core.Models;
using Aggregator.Worker.Diagnostics;

namespace UnitTests.Worker.Diagnostics;

public class ProcessingStatsTests
{
    [Test]
    public void IncrementRawReceived_IncreasesGlobalAndPerSourceCounters()
    {
        var sut = new ProcessingStats();

        sut.IncrementRawReceived(ExchangeSource.Binance);
        sut.IncrementRawReceived(ExchangeSource.Binance);
        sut.IncrementRawReceived(ExchangeSource.Coinbase);

        var snapshot = sut.Snapshot();

        Assert.That(snapshot.RawReceived, Is.EqualTo(3));
        Assert.That(snapshot.Connections["Binance"].RawReceived, Is.EqualTo(2));
        Assert.That(snapshot.Connections["Coinbase"].RawReceived, Is.EqualTo(1));
    }

    [Test]
    public void IncrementNormalizedOk_IncreasesGlobalAndPerSourceCounters()
    {
        var sut = new ProcessingStats();

        sut.IncrementNormalizedOk(ExchangeSource.Binance);
        sut.IncrementNormalizedOk(ExchangeSource.Kraken);
        sut.IncrementNormalizedOk(ExchangeSource.Kraken);

        var snapshot = sut.Snapshot();

        Assert.That(snapshot.NormalizedOk, Is.EqualTo(3));
        Assert.That(snapshot.Connections["Binance"].NormalizedOk, Is.EqualTo(1));
        Assert.That(snapshot.Connections["Kraken"].NormalizedOk, Is.EqualTo(2));
    }

    [Test]
    public void IncrementReconnectAttempt_TracksPerSourceCycle()
    {
        var sut = new ProcessingStats();

        sut.IncrementReconnectAttempt(ExchangeSource.Coinbase);
        sut.IncrementReconnectAttempt(ExchangeSource.Coinbase);
        sut.IncrementReconnectAttempt(ExchangeSource.Binance);

        var snapshot = sut.Snapshot();

        Assert.That(snapshot.ReconnectAttemptsTotal, Is.EqualTo(3));
        Assert.That(snapshot.Connections["Coinbase"].ReconnectAttemptsTotal, Is.EqualTo(2));
        Assert.That(snapshot.Connections["Coinbase"].ReconnectAttemptsCurrentCycle, Is.EqualTo(2));
        Assert.That(snapshot.Connections["Binance"].ReconnectAttemptsCurrentCycle, Is.EqualTo(1));
    }

    [Test]
    public void ResetReconnectCycleAttempts_ResetsOnlyRequestedSource()
    {
        var sut = new ProcessingStats();

        sut.IncrementReconnectAttempt(ExchangeSource.Binance);
        sut.IncrementReconnectAttempt(ExchangeSource.Coinbase);
        sut.IncrementReconnectAttempt(ExchangeSource.Coinbase);
        sut.ResetReconnectCycleAttempts(ExchangeSource.Coinbase);

        var snapshot = sut.Snapshot();

        Assert.That(snapshot.Connections["Coinbase"].ReconnectAttemptsCurrentCycle, Is.EqualTo(0));
        Assert.That(snapshot.Connections["Binance"].ReconnectAttemptsCurrentCycle, Is.EqualTo(1));
    }

    [Test]
    public void Snapshot_ContainsGlobalAndPerConnectionSections()
    {
        var sut = new ProcessingStats();

        sut.MarkStarted();
        sut.IncrementRawReceived(ExchangeSource.Binance);
        sut.IncrementChannelRead(ExchangeSource.Binance);
        sut.IncrementNormalizedFailed(ExchangeSource.Kraken);
        sut.IncrementConnectFailures(ExchangeSource.Kraken);
        sut.SetLastReconnectDelayMs(ExchangeSource.Kraken, 3000);
        sut.IncrementBatchesFlushed();
        sut.SetLastBatchSize(42);

        var snapshot = sut.Snapshot();

        Assert.That(snapshot.RawReceived, Is.EqualTo(1));
        Assert.That(snapshot.ChannelRead, Is.EqualTo(1));
        Assert.That(snapshot.NormalizedFailed, Is.EqualTo(1));
        Assert.That(snapshot.ConnectFailures, Is.EqualTo(1));
        Assert.That(snapshot.BatchesFlushedTotal, Is.EqualTo(1));
        Assert.That(snapshot.LastBatchSize, Is.EqualTo(42));
        Assert.That(snapshot.Connections.ContainsKey("Binance"), Is.True);
        Assert.That(snapshot.Connections.ContainsKey("Kraken"), Is.True);
        Assert.That(snapshot.Connections["Kraken"].LastReconnectDelayMs, Is.EqualTo(3000));
    }
}
