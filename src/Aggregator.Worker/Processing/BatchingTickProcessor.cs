using Aggregator.Core.Models;
using Aggregator.Core.Persistence;
using Aggregator.Worker.Diagnostics;

namespace Aggregator.Worker.Processing;

public sealed class BatchingTickProcessor
{
    private readonly ITradeTickSink _sink;
    private readonly ProcessingStats _stats;
    private readonly int _batchSize;
    private readonly TimeSpan _batchTimeout;
    private readonly List<TradeTick> _buffer;
    private DateTimeOffset _lastFlushAtUtc;

    public BatchingTickProcessor(
        ITradeTickSink sink,
        BatchingOptions options,
        ProcessingStats stats)
    {
        _sink = sink;
        _stats = stats;
        _batchSize = Math.Max(1, options.BatchSize);
        _batchTimeout = TimeSpan.FromMilliseconds(Math.Max(1, options.BatchTimeoutMs));
        _buffer = new List<TradeTick>(_batchSize);
        _lastFlushAtUtc = DateTimeOffset.UtcNow;
    }

    public async Task AddAsync(TradeTick tick, CancellationToken cancellationToken)
    {
        _buffer.Add(tick);
        var timeoutReached = DateTimeOffset.UtcNow - _lastFlushAtUtc >= _batchTimeout;
        if (_buffer.Count >= _batchSize || timeoutReached)
        {
            await FlushAsync(cancellationToken);
        }
    }

    public async Task FlushAsync(CancellationToken cancellationToken)
    {
        if (_buffer.Count == 0)
        {
            return;
        }

        var batchSize = _buffer.Count;
        var batch = _buffer.ToArray();
        _buffer.Clear();
        _lastFlushAtUtc = DateTimeOffset.UtcNow;
        await _sink.WriteBatchAsync(batch, cancellationToken);
        _stats.SetLastBatchSize(batchSize);
        _stats.IncrementBatchesFlushed();
    }
}
