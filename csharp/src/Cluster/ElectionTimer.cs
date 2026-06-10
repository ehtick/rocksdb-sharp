#if NET5_0_OR_GREATER
using System;
using System.Threading;

namespace RocksDbSharp;

/// <summary>
/// Randomized election timer used by Raft followers and candidates.
///
/// The timer is reset by leader heartbeats and by granted votes. When it
/// elapses, <see cref="OnTimeout"/> fires - typically transitioning the node
/// to Candidate and starting a new election.
/// </summary>
public sealed class ElectionTimer : IDisposable
{
    private readonly Action _onTimeout;
    private readonly int _minMs;
    private readonly int _maxMs;
    private readonly Random _rng;
    private readonly object _gate = new();

    private Timer? _timer;
    private bool _disposed;
    private long _lastResetTicks;

    public ElectionTimer(int minMs, int maxMs, Action onTimeout)
    {
        _minMs = minMs;
        _maxMs = maxMs;
        _onTimeout = onTimeout ?? throw new ArgumentNullException(nameof(onTimeout));
        _rng = new Random(Environment.TickCount ^ Thread.CurrentThread.ManagedThreadId);
    }

    public DateTime LastResetUtc => new DateTime(Interlocked.Read(ref _lastResetTicks), DateTimeKind.Utc);

    public void Start()
    {
        Reset();
    }

    public void Reset()
    {
        lock (_gate)
        {
            if (_disposed) return;
            int next = _rng.Next(_minMs, _maxMs + 1);
            Interlocked.Exchange(ref _lastResetTicks, DateTime.UtcNow.Ticks);
            if (_timer == null)
            {
                _timer = new Timer(_ => SafeTimeout(), null, next, Timeout.Infinite);
            }
            else
            {
                _timer.Change(next, Timeout.Infinite);
            }
        }
    }

    public void Pause()
    {
        lock (_gate)
        {
            _timer?.Change(Timeout.Infinite, Timeout.Infinite);
        }
    }

    private void SafeTimeout()
    {
        try { _onTimeout(); }
        catch { /* swallowed; the caller logs */ }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            _disposed = true;
            _timer?.Dispose();
            _timer = null;
        }
    }
}
#endif
