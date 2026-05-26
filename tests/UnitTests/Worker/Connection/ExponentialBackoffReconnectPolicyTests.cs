using Aggregator.Worker.Configuration;
using Aggregator.Worker.Connection;

namespace UnitTests.Worker.Connection;

public class ExponentialBackoffReconnectPolicyTests
{
    [Test]
    public void ShouldReconnect_WithUnlimitedAttempts_ReturnsTrue()
    {
        var sut = CreatePolicy(maxAttempts: 0, jitterRatio: 0d);

        Assert.That(sut.ShouldReconnect(1), Is.True);
        Assert.That(sut.ShouldReconnect(1000), Is.True);
    }

    [Test]
    public void ShouldReconnect_WithLimitedAttempts_StopsAfterLimit()
    {
        var sut = CreatePolicy(maxAttempts: 3, jitterRatio: 0d);

        Assert.That(sut.ShouldReconnect(1), Is.True);
        Assert.That(sut.ShouldReconnect(3), Is.True);
        Assert.That(sut.ShouldReconnect(4), Is.False);
    }

    [Test]
    public void CalculateDelayMs_WithoutJitter_UsesExponentialBackoff()
    {
        var sut = CreatePolicy(delayMs: 3000, maxDelayMs: 30000, jitterRatio: 0d);

        Assert.That(sut.CalculateDelayMs(1), Is.EqualTo(3000));
        Assert.That(sut.CalculateDelayMs(2), Is.EqualTo(6000));
        Assert.That(sut.CalculateDelayMs(3), Is.EqualTo(12000));
    }

    [Test]
    public void CalculateDelayMs_RespectsMaxDelay()
    {
        var sut = CreatePolicy(delayMs: 3000, maxDelayMs: 10000, jitterRatio: 0d);

        Assert.That(sut.CalculateDelayMs(10), Is.EqualTo(10000));
    }

    [Test]
    public void ConnectTimeoutMs_UsesConfiguredValue()
    {
        var sut = CreatePolicy(connectTimeoutMs: 7000, jitterRatio: 0d);

        Assert.That(sut.ConnectTimeoutMs, Is.EqualTo(7000));
    }

    private static ExponentialBackoffReconnectPolicy CreatePolicy(
        int maxAttempts = 0,
        int connectTimeoutMs = 5000,
        int delayMs = 3000,
        int maxDelayMs = 30000,
        double jitterRatio = 0.2d)
    {
        return new ExponentialBackoffReconnectPolicy(new ReconnectOptions
        {
            MaxAttempts = maxAttempts,
            ConnectTimeoutMs = connectTimeoutMs,
            DelayMs = delayMs,
            MaxDelayMs = maxDelayMs,
            JitterRatio = jitterRatio
        });
    }
}
