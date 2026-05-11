#if NET5_0_OR_GREATER
using System;
using System.Threading;

namespace RocksDbSharp;

/// <summary>
/// Per-peer tracking maintained by the leader.
///
/// nextIndex / matchIndex mirror the variables from Raft §5.3 but, because our
/// "log" is the RocksDB WAL keyed by sequence number, we name them with the
/// "Seq" suffix.
/// </summary>
public sealed class PeerState
{
    public ClusterMember Member { get; }

    private long _matchSeq;
    private long _nextSeq;
    private long _lastSuccessfulContactTicks;
    private int _reachable; // 1/0

    public PeerState(ClusterMember member, ulong initialNextSeq)
    {
        Member = member;
        _matchSeq = 0;
        _nextSeq = (long)initialNextSeq;
        _reachable = 0;
    }

    public ulong MatchSeq => (ulong)Interlocked.Read(ref _matchSeq);
    public ulong NextSeq => (ulong)Interlocked.Read(ref _nextSeq);
    public bool IsReachable => Volatile.Read(ref _reachable) == 1;
    public DateTime LastSuccessfulContactUtc =>
        new DateTime(Interlocked.Read(ref _lastSuccessfulContactTicks), DateTimeKind.Utc);

    public void OnAppendSucceeded(ulong matchSeq)
    {
        Interlocked.Exchange(ref _matchSeq, (long)matchSeq);
        Interlocked.Exchange(ref _nextSeq, (long)matchSeq + 1);
        Interlocked.Exchange(ref _lastSuccessfulContactTicks, DateTime.UtcNow.Ticks);
        Volatile.Write(ref _reachable, 1);
    }

    public void OnAppendFailed()
    {
        Volatile.Write(ref _reachable, 0);
    }

    public void OnHeartbeatAcknowledged()
    {
        Interlocked.Exchange(ref _lastSuccessfulContactTicks, DateTime.UtcNow.Ticks);
        Volatile.Write(ref _reachable, 1);
    }

    public void RewindOnConflict(ulong newNextSeq)
    {
        Interlocked.Exchange(ref _nextSeq, (long)newNextSeq);
    }
}
#endif
