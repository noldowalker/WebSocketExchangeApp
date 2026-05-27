using Aggregator.Core.Persistence;
using Aggregator.Domain.Models;
using Aggregator.Application.Connection;
using Aggregator.Application.Configuration;
using Aggregator.Application.Diagnostics;
using Aggregator.Application.Models;
using Aggregator.Application.Normalization;
using Aggregator.Application.Processing;
using Aggregator.Application.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;

namespace UnitTests.Application;

public class ExchangeTicksIngestionWorkerTests
{
    [Test]
    public async Task ExecuteAsync_WhenMessageIsNormalized_WritesTickToSink()
    {
        var sinkMock = new Mock<ITradeTickSink>();
        var reconnectPolicyMock = new Mock<IReconnectPolicy>();
        var routerMock = new Mock<IExchangeTickNormalizerRouter>();
        var transport = new FakeTransport(["payload"]);
        var transportFactoryMock = CreateFactory(transport);
        var tick = new TradeTick("binance", "BTCUSDT", 100m, 1m, DateTimeOffset.UtcNow);
        TradeTick? outTick = tick;

        routerMock
            .Setup(x => x.TryNormalize(ExchangeSource.Binance, "payload", out outTick))
            .Returns(true);
        reconnectPolicyMock.SetupGet(x => x.ConnectTimeoutMs).Returns(5000);
        reconnectPolicyMock.Setup(x => x.ShouldReconnect(1)).Returns(false);

        var worker = CreateWorker(
            reconnectPolicyMock.Object,
            transportFactoryMock.Object,
            routerMock.Object,
            sinkMock.Object);

        await worker.RunForTestsAsync(CancellationToken.None);

        sinkMock.Verify(
            x => x.WriteBatchAsync(
                It.Is<IReadOnlyCollection<TradeTick>>(batch => batch.Count == 1 && batch.Single() == tick),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Test]
    public async Task ExecuteAsync_WhenMessageIsInvalid_DoesNotWriteTickToSink()
    {
        var sinkMock = new Mock<ITradeTickSink>();
        var reconnectPolicyMock = new Mock<IReconnectPolicy>();
        var routerMock = new Mock<IExchangeTickNormalizerRouter>();
        var transport = new FakeTransport(["payload"]);
        var transportFactoryMock = CreateFactory(transport);
        TradeTick? outTick = null;

        routerMock
            .Setup(x => x.TryNormalize(ExchangeSource.Binance, "payload", out outTick))
            .Returns(false);
        reconnectPolicyMock.SetupGet(x => x.ConnectTimeoutMs).Returns(5000);
        reconnectPolicyMock.Setup(x => x.ShouldReconnect(1)).Returns(false);

        var worker = CreateWorker(
            reconnectPolicyMock.Object,
            transportFactoryMock.Object,
            routerMock.Object,
            sinkMock.Object);

        await worker.RunForTestsAsync(CancellationToken.None);

        sinkMock.Verify(
            x => x.WriteBatchAsync(It.IsAny<IReadOnlyCollection<TradeTick>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Test]
    public async Task ExecuteAsync_WhenConnectFails_TracksFailureAndReconnectAttempt()
    {
        var sinkMock = new Mock<ITradeTickSink>();
        var reconnectPolicyMock = new Mock<IReconnectPolicy>();
        var routerMock = new Mock<IExchangeTickNormalizerRouter>();
        var transport = new FakeTransport([], new InvalidOperationException("connect failed"));
        var transportFactoryMock = CreateFactory(transport);
        var stats = new ProcessingStats();

        reconnectPolicyMock.SetupGet(x => x.ConnectTimeoutMs).Returns(5000);
        reconnectPolicyMock.Setup(x => x.ShouldReconnect(1)).Returns(false);

        var worker = CreateWorker(
            reconnectPolicyMock.Object,
            transportFactoryMock.Object,
            routerMock.Object,
            sinkMock.Object,
            stats);

        await worker.RunForTestsAsync(CancellationToken.None);

        var snapshot = stats.Snapshot();
        Assert.That(snapshot.ConnectFailures, Is.EqualTo(1));
        Assert.That(snapshot.ReconnectAttemptsTotal, Is.EqualTo(1));
        Assert.That(snapshot.Connections["Binance"].ConnectFailures, Is.EqualTo(1));
    }

    private static TestableExchangeTicksIngestionWorker CreateWorker(
        IReconnectPolicy reconnectPolicy,
        IExchangeWebSocketTransportFactory transportFactory,
        IExchangeTickNormalizerRouter router,
        ITradeTickSink sink,
        ProcessingStats? stats = null)
    {
        var processingStats = stats ?? new ProcessingStats();
        var scopeFactory = CreateScopeFactory(router);
        var reconnectPolicyFactory = new Mock<IReconnectPolicyFactory>();
        reconnectPolicyFactory
            .Setup(x => x.Create(It.IsAny<ReconnectOptions>()))
            .Returns(reconnectPolicy);

        var batchingProcessor = new BatchingTickProcessor(
            sink,
            new BatchingOptions
            {
                BatchSize = 10,
                BatchTimeoutMs = 1000
            },
            processingStats);

        return new TestableExchangeTicksIngestionWorker(
            Mock.Of<ILogger<Aggregator.Application.ExchangeTicksIngestionWorker>>(),
            [new ExchangeConnectionOptions
            {
                Url = "ws://localhost:5000/ws/binance",
                Source = ExchangeSource.Binance,
                Reconnect = new ReconnectOptions()
            }],
            scopeFactory.Object,
            reconnectPolicyFactory.Object,
            transportFactory,
            batchingProcessor,
            processingStats);
    }

    private static Mock<IExchangeWebSocketTransportFactory> CreateFactory(IExchangeWebSocketTransport transport)
    {
        var factoryMock = new Mock<IExchangeWebSocketTransportFactory>();
        factoryMock.Setup(x => x.Create()).Returns(transport);
        return factoryMock;
    }

    private static Mock<IServiceScopeFactory> CreateScopeFactory(IExchangeTickNormalizerRouter router)
    {
        var serviceProvider = new Mock<IServiceProvider>();
        serviceProvider
            .Setup(x => x.GetService(typeof(IExchangeTickNormalizerRouter)))
            .Returns(router);

        var scope = new Mock<IServiceScope>();
        scope.SetupGet(x => x.ServiceProvider).Returns(serviceProvider.Object);

        var scopeFactory = new Mock<IServiceScopeFactory>();
        scopeFactory.Setup(x => x.CreateScope()).Returns(scope.Object);
        return scopeFactory;
    }

    private sealed class TestableExchangeTicksIngestionWorker : Aggregator.Application.ExchangeTicksIngestionWorker
    {
        public TestableExchangeTicksIngestionWorker(
            ILogger<Aggregator.Application.ExchangeTicksIngestionWorker> logger,
            IReadOnlyList<ExchangeConnectionOptions> connections,
            IServiceScopeFactory scopeFactory,
            IReconnectPolicyFactory reconnectPolicyFactory,
            IExchangeWebSocketTransportFactory transportFactory,
            BatchingTickProcessor batchingTickProcessor,
            ProcessingStats processingStats)
            : base(
                logger,
                connections,
                scopeFactory,
                reconnectPolicyFactory,
                transportFactory,
                batchingTickProcessor,
                processingStats)
        {
        }

        public Task RunForTestsAsync(CancellationToken cancellationToken)
        {
            return ExecuteAsync(cancellationToken);
        }
    }

    private sealed class FakeTransport : IExchangeWebSocketTransport
    {
        private readonly IReadOnlyList<string> _messages;
        private readonly Exception? _connectException;

        public FakeTransport(IReadOnlyList<string> messages, Exception? connectException = null)
        {
            _messages = messages;
            _connectException = connectException;
        }

        public Task ConnectAsync(Uri uri, CancellationToken cancellationToken)
        {
            if (_connectException is not null)
            {
                throw _connectException;
            }

            return Task.CompletedTask;
        }

        public async IAsyncEnumerable<string> ReadMessagesAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            foreach (var message in _messages)
            {
                await Task.Yield();
                yield return message;
            }
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
