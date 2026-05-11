#if NET5_0_OR_GREATER
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace RocksDbSharp;

/// <summary>
/// Transport-agnostic Raft orchestrator. The MagicOnion service in the test
/// project owns the network and delegates incoming RPCs into this class
/// through its three RPC-level entry points:
///   - <see cref="HandleRequestVote"/>
///   - <see cref="HandleAppendEntries"/>
///   - <see cref="HandleObservedLeader"/>
///
/// Outgoing RPCs are issued via the <see cref="IPeerTransport"/> delegate that
/// the host provides during construction. This keeps the cluster library free
/// of MagicOnion / gRPC types.
/// </summary>
public sealed class RaftClusterNode : IDisposable
{
    private readonly RaftConfig _config;
    private readonly RaftPersistentState _state;
    private readonly RocksDb _db;
    private readonly IPeerTransport _transport;
    private readonly ElectionTimer _electionTimer;
    private readonly object _gate = new();
    private readonly Dictionary<string, PeerState> _peers;
    private readonly CancellationTokenSource _cts = new();

    private RaftRole _role;
    private string? _currentLeaderId;
    private ulong _commitSeq;
    private Task? _leaderLoop;
    private int _bootstrapInSyncFollowers;

    public event EventHandler<LeaderChangedEventArgs>? LeaderChanged;
    public event EventHandler<RoleChangedEventArgs>? RoleChanged;
    public event EventHandler<ModeChangedEventArgs>? ModeChanged;

    public RaftClusterNode(RaftConfig config, RaftPersistentState state, RocksDb db, IPeerTransport transport)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _state = state ?? throw new ArgumentNullException(nameof(state));
        _db = db ?? throw new ArgumentNullException(nameof(db));
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));

        _peers = _config.Peers.ToDictionary(
            p => p.NodeId,
            p => new PeerState(p, db.GetLatestSequenceNumber() + 1));

        _electionTimer = new ElectionTimer(_config.ElectionTimeoutMinMs, _config.ElectionTimeoutMaxMs, OnElectionTimeout);

        if (_config.BootstrapPrimary && _state.Mode == ClusterMode.Bootstrap)
        {
            // Bootstrap primary: act as leader without holding an election.
            // Term is recorded on first promotion to cluster-mode.
            _role = RaftRole.Leader;
            _currentLeaderId = _config.NodeId;
            if (_state.CurrentTerm == 0)
            {
                _state.UpdateTermAndVote(1, _config.NodeId);
                _state.RecordTermRange(_db.GetLatestSequenceNumber() + 1, 1);
            }
        }
        else
        {
            _role = RaftRole.Follower;
        }
    }

    public string NodeId => _config.NodeId;
    public RaftRole Role { get { lock (_gate) return _role; } }
    public long CurrentTerm => _state.CurrentTerm;
    public string? CurrentLeaderId { get { lock (_gate) return _currentLeaderId; } }
    public ClusterMode Mode => _state.Mode;
    public ulong CommitSeq => Volatile.Read(ref _commitSeq);
    public IReadOnlyCollection<PeerState> Peers => _peers.Values.ToList();

    public void Start()
    {
        if (_role == RaftRole.Leader)
        {
            StartLeaderLoop();
        }
        else
        {
            _electionTimer.Start();
        }
    }

    public void Dispose()
    {
        _cts.Cancel();
        _electionTimer.Dispose();
    }

    // ===== Incoming RPC handlers =====

    public RequestVoteResult HandleRequestVote(long term, string candidateId, ulong lastLogSeq, long lastLogTerm)
    {
        lock (_gate)
        {
            if (term > _state.CurrentTerm)
            {
                _state.UpdateTermAndVote(term, null);
                BecomeFollower(null);
            }

            if (term < _state.CurrentTerm)
                return new RequestVoteResult(_state.CurrentTerm, false);

            // In Bootstrap mode we do not honor election attempts: the bootstrap
            // primary is fixed. Followers will reject votes so a partition does
            // not accidentally produce a leader before initialization completes.
            if (_state.Mode == ClusterMode.Bootstrap)
                return new RequestVoteResult(_state.CurrentTerm, false);

            var votedFor = _state.VotedFor;
            if (votedFor != null && votedFor != candidateId)
                return new RequestVoteResult(_state.CurrentTerm, false);

            // candidate's log must be at least as up-to-date as ours
            ulong myLastSeq = _db.GetLatestSequenceNumber();
            long myLastTerm = _state.Terms.GetTermForSeq(myLastSeq);
            bool upToDate = lastLogTerm > myLastTerm
                            || (lastLogTerm == myLastTerm && lastLogSeq >= myLastSeq);
            if (!upToDate) return new RequestVoteResult(_state.CurrentTerm, false);

            _state.UpdateTermAndVote(term, candidateId);
            _electionTimer.Reset();
            return new RequestVoteResult(_state.CurrentTerm, true);
        }
    }

    public AppendEntriesResult HandleAppendEntries(
        long term,
        string leaderId,
        ulong prevLogSeq,
        long prevLogTerm,
        ulong leaderCommit,
        ClusterMode leaderMode)
    {
        lock (_gate)
        {
            if (term > _state.CurrentTerm)
                _state.UpdateTermAndVote(term, null);

            if (term < _state.CurrentTerm)
                return new AppendEntriesResult(_state.CurrentTerm, false, _db.GetLatestSequenceNumber(), "stale term");

            BecomeFollower(leaderId);
            _electionTimer.Reset();

            // Adopt cluster mode if the leader has promoted.
            if (leaderMode == ClusterMode.Cluster && _state.Mode == ClusterMode.Bootstrap)
            {
                _state.SetMode(ClusterMode.Cluster);
                ModeChanged?.Invoke(this, new ModeChangedEventArgs(ClusterMode.Cluster));
            }

            // log consistency check
            ulong myLatest = _db.GetLatestSequenceNumber();
            if (prevLogSeq > 0)
            {
                if (prevLogSeq > myLatest)
                    return new AppendEntriesResult(_state.CurrentTerm, false, myLatest, "behind");

                long myTerm = _state.Terms.GetTermForSeq(prevLogSeq);
                if (myTerm != prevLogTerm)
                    return new AppendEntriesResult(_state.CurrentTerm, false, myLatest, "term-mismatch");
            }

            // commit advance
            ulong newCommit = Math.Min(leaderCommit, myLatest);
            if (newCommit > Volatile.Read(ref _commitSeq))
                Volatile.Write(ref _commitSeq, newCommit);

            return new AppendEntriesResult(_state.CurrentTerm, true, myLatest, null);
        }
    }

    /// <summary>
    /// Followers call this when they observe a higher term while consuming
    /// WAL batches (the existing replication stream is leader → follower, so
    /// the leader tags its current term on the heartbeat).
    /// </summary>
    public void HandleObservedLeader(string leaderId, long term)
    {
        lock (_gate)
        {
            if (term >= _state.CurrentTerm)
            {
                _state.UpdateTermAndVote(term, null);
                BecomeFollower(leaderId);
                _electionTimer.Reset();
            }
        }
    }

    // ===== Leader-only: log replication advance =====

    public void RecordFollowerProgress(string nodeId, ulong matchSeq)
    {
        if (!_peers.TryGetValue(nodeId, out var peer)) return;
        peer.OnAppendSucceeded(matchSeq);
        TryAdvanceCommit();
        MaybePromoteToCluster();
    }

    public void RecordFollowerHeartbeat(string nodeId)
    {
        if (_peers.TryGetValue(nodeId, out var peer)) peer.OnHeartbeatAcknowledged();
    }

    public void RecordFollowerUnreachable(string nodeId)
    {
        if (_peers.TryGetValue(nodeId, out var peer)) peer.OnAppendFailed();
    }

    /// <summary>
    /// Called by the leader's host loop to read the leader's "current commit"
    /// from a follower's perspective, given the per-peer match seq.
    /// </summary>
    private void TryAdvanceCommit()
    {
        if (_role != RaftRole.Leader) return;
        // sort matchSeqs descending; quorum-th value is the new commitIndex
        var seqs = _peers.Values.Select(p => p.MatchSeq).ToList();
        seqs.Add(_db.GetLatestSequenceNumber()); // include self
        seqs.Sort((a, b) => b.CompareTo(a));
        int q = _config.Quorum;
        if (seqs.Count >= q)
        {
            ulong newCommit = seqs[q - 1];
            if (newCommit > Volatile.Read(ref _commitSeq))
                Volatile.Write(ref _commitSeq, newCommit);
        }
    }

    private void MaybePromoteToCluster()
    {
        if (_state.Mode != ClusterMode.Bootstrap) return;
        if (_role != RaftRole.Leader) return;

        ulong latest = _db.GetLatestSequenceNumber();
        ulong threshold = _config.BootstrapInSyncLagThreshold;
        int inSync = 1; // self
        foreach (var p in _peers.Values)
        {
            if (p.MatchSeq + threshold >= latest && p.MatchSeq > 0) inSync++;
        }
        Volatile.Write(ref _bootstrapInSyncFollowers, inSync);
        if (inSync >= _config.BootstrapPromotionQuorum)
        {
            _state.SetMode(ClusterMode.Cluster);
            ModeChanged?.Invoke(this, new ModeChangedEventArgs(ClusterMode.Cluster));
        }
    }

    public int BootstrapInSyncCount => Volatile.Read(ref _bootstrapInSyncFollowers);

    // ===== State transitions =====

    private void BecomeFollower(string? leaderId)
    {
        bool roleChanged = _role != RaftRole.Follower;
        bool leaderChanged = _currentLeaderId != leaderId;
        _role = RaftRole.Follower;
        _currentLeaderId = leaderId;
        if (_leaderLoop != null)
        {
            // leader loop watches _role and exits on its own
            _leaderLoop = null;
        }
        if (roleChanged) RoleChanged?.Invoke(this, new RoleChangedEventArgs(_role, _state.CurrentTerm));
        if (leaderChanged) LeaderChanged?.Invoke(this, new LeaderChangedEventArgs(leaderId, _state.CurrentTerm));
        _electionTimer.Reset();
    }

    private void BecomeCandidate()
    {
        _role = RaftRole.Candidate;
        _state.UpdateTermAndVote(_state.CurrentTerm + 1, _config.NodeId);
        _currentLeaderId = null;
        RoleChanged?.Invoke(this, new RoleChangedEventArgs(_role, _state.CurrentTerm));
        LeaderChanged?.Invoke(this, new LeaderChangedEventArgs(null, _state.CurrentTerm));
    }

    private void BecomeLeader()
    {
        _role = RaftRole.Leader;
        _currentLeaderId = _config.NodeId;
        ulong nextSeq = _db.GetLatestSequenceNumber() + 1;
        foreach (var p in _peers.Values) p.RewindOnConflict(nextSeq);
        _state.RecordTermRange(nextSeq, _state.CurrentTerm);
        RoleChanged?.Invoke(this, new RoleChangedEventArgs(_role, _state.CurrentTerm));
        LeaderChanged?.Invoke(this, new LeaderChangedEventArgs(_config.NodeId, _state.CurrentTerm));
        _electionTimer.Pause();
        StartLeaderLoop();
    }

    private void StartLeaderLoop()
    {
        _leaderLoop = Task.Run(() => LeaderHeartbeatLoopAsync(_cts.Token));
    }

    private async Task LeaderHeartbeatLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                lock (_gate) { if (_role != RaftRole.Leader) break; }

                ulong latest = _db.GetLatestSequenceNumber();
                ulong commit = Volatile.Read(ref _commitSeq);
                long term = _state.CurrentTerm;
                var leaderMode = _state.Mode;

                foreach (var peer in _peers.Values)
                {
                    ulong prevLogSeq = peer.NextSeq > 0 ? peer.NextSeq - 1 : 0;
                    long prevLogTerm = _state.Terms.GetTermForSeq(prevLogSeq);
                    var beat = new HeartbeatPayload(term, _config.NodeId, prevLogSeq, prevLogTerm, commit, leaderMode);
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            var resp = await _transport.SendHeartbeatAsync(peer.Member, beat, ct).ConfigureAwait(false);
                            if (resp == null)
                            {
                                peer.OnAppendFailed();
                                return;
                            }
                            if (resp.Term > _state.CurrentTerm)
                            {
                                lock (_gate)
                                {
                                    _state.UpdateTermAndVote(resp.Term, null);
                                    BecomeFollower(null);
                                }
                                return;
                            }
                            if (resp.Success)
                            {
                                peer.OnAppendSucceeded(resp.LastSeq);
                                TryAdvanceCommit();
                                MaybePromoteToCluster();
                            }
                            else
                            {
                                // conflict – pull nextSeq back; the WAL streamer
                                // running separately will fall back to snapshot.
                                peer.RewindOnConflict(Math.Max(1, resp.LastSeq));
                            }
                        }
                        catch { peer.OnAppendFailed(); }
                    }, ct);
                }

                await Task.Delay(_config.HeartbeatIntervalMs, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { }
    }

    private void OnElectionTimeout()
    {
        lock (_gate)
        {
            if (_role == RaftRole.Leader) return;
            if (_state.Mode == ClusterMode.Bootstrap)
            {
                // While in Bootstrap mode the cluster has not yet promoted; only
                // the bootstrap primary may be leader, so reset and wait.
                _electionTimer.Reset();
                return;
            }
            BecomeCandidate();
        }

        _ = Task.Run(RunElectionAsync);
    }

    private async Task RunElectionAsync()
    {
        long term;
        ulong lastLogSeq;
        long lastLogTerm;
        List<ClusterMember> peers;
        lock (_gate)
        {
            term = _state.CurrentTerm;
            lastLogSeq = _db.GetLatestSequenceNumber();
            lastLogTerm = _state.Terms.GetTermForSeq(lastLogSeq);
            peers = _peers.Values.Select(p => p.Member).ToList();
            _electionTimer.Reset();
        }

        int votes = 1; // self
        int needed = _config.Quorum;
        var tasks = peers.Select(async p =>
        {
            try
            {
                var resp = await _transport.SendRequestVoteAsync(p, term, _config.NodeId, lastLogSeq, lastLogTerm, _cts.Token)
                                           .ConfigureAwait(false);
                return resp;
            }
            catch { return null; }
        }).ToList();

        while (tasks.Count > 0)
        {
            var done = await Task.WhenAny(tasks).ConfigureAwait(false);
            tasks.Remove(done);
            var resp = await done.ConfigureAwait(false);
            if (resp == null) continue;
            if (resp.Term > term)
            {
                lock (_gate)
                {
                    _state.UpdateTermAndVote(resp.Term, null);
                    BecomeFollower(null);
                }
                return;
            }
            if (resp.VoteGranted)
            {
                votes++;
                if (votes >= needed)
                {
                    lock (_gate)
                    {
                        // Make sure we are still candidate for this term
                        if (_role != RaftRole.Candidate || _state.CurrentTerm != term) return;
                        BecomeLeader();
                    }
                    return;
                }
            }
        }
        // election failed; election timer will fire again
    }
}

public sealed record RequestVoteResult(long Term, bool VoteGranted);
public sealed record AppendEntriesResult(long Term, bool Success, ulong LastSeq, string? Reason);
public sealed record HeartbeatPayload(long Term, string LeaderId, ulong PrevLogSeq, long PrevLogTerm, ulong LeaderCommit, ClusterMode LeaderMode);

public interface IPeerTransport
{
    Task<RequestVoteResult?> SendRequestVoteAsync(ClusterMember peer, long term, string candidateId, ulong lastLogSeq, long lastLogTerm, CancellationToken ct);
    Task<AppendEntriesResult?> SendHeartbeatAsync(ClusterMember peer, HeartbeatPayload payload, CancellationToken ct);
}
#endif
