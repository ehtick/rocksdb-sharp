#if NET5_0_OR_GREATER
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RocksDbSharp;

/// <summary>
/// Transport-agnostic Raft orchestrator. The MagicOnion service in the test
/// project owns the network and delegates incoming RPCs into this class
/// through its RPC-level entry points:
///   - <see cref="HandleRequestVote"/>
///   - <see cref="HandleAppendEntries"/>
///   - <see cref="HandleObservedLeader"/>
///   - <see cref="CheckFollowerLogConsistency"/>
///
/// Outgoing RPCs are issued via the <see cref="IPeerTransport"/> delegate that
/// the host provides during construction. This keeps the cluster library free
/// of MagicOnion / gRPC types.
/// </summary>
public sealed class RaftClusterNode : IDisposable
{
    /// <summary>
    /// Key of the marker entry a leader writes immediately after winning an
    /// election. Raft (§5.4.2) forbids committing entries from previous terms
    /// by counting replicas, so replicating one current-term entry right away
    /// lets the commit index advance without waiting for a user write.
    /// </summary>
    public static readonly byte[] LeaderNoOpKey = Encoding.UTF8.GetBytes("__raft_noop");

    private readonly RaftConfig _config;
    private readonly RaftPersistentState _state;
    private RocksDb _db;
    private readonly IPeerTransport _transport;
    private readonly ElectionTimer _electionTimer;
    private readonly object _gate = new();
    private readonly Dictionary<string, PeerState> _peers;
    private readonly CancellationTokenSource _cts = new();

    private RaftRole _role;
    private string? _currentLeaderId;
    private ulong _commitSeq;
    private CancellationTokenSource? _leaderCts;
    private bool _resyncing;
    private int _bootstrapInSyncFollowers;

    public event EventHandler<LeaderChangedEventArgs>? LeaderChanged;
    public event EventHandler<RoleChangedEventArgs>? RoleChanged;
    public event EventHandler<ModeChangedEventArgs>? ModeChanged;

    /// <summary>
    /// Raised when this node detects that its log conflicts with the current
    /// leader's log (a term mismatch at a sequence number both logs contain).
    /// The RocksDB WAL cannot be truncated, so the host must rebuild this
    /// replica from a leader snapshot: call <see cref="BeginResync"/>, dispose
    /// the database, restore a snapshot, then call <see cref="CompleteResync"/>.
    /// </summary>
    public event EventHandler? ResyncRequired;

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
    public bool IsResyncing { get { lock (_gate) return _resyncing; } }
    public int BootstrapInSyncCount => Volatile.Read(ref _bootstrapInSyncFollowers);

    public long GetTermForSeq(ulong seq) => _state.Terms.GetTermForSeq(seq);

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
            // While our log is being rebuilt from a snapshot we have nothing to
            // compare the candidate's log against; granting a vote here could
            // help elect a candidate that lacks committed entries.
            if (_resyncing)
                return new RequestVoteResult(_state.CurrentTerm, false);

            if (term > _state.CurrentTerm)
            {
                // Elections can only be started by nodes already in Cluster
                // mode, so a higher-term RequestVote proves the promotion
                // quorum was reached somewhere. Adopt Cluster mode, otherwise
                // a cluster whose leader promoted and then died before
                // broadcasting the new mode could never elect a successor.
                if (_state.Mode == ClusterMode.Bootstrap)
                    PromoteToClusterLocked();
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
        AppendEntriesResult result;
        bool conflict = false;
        lock (_gate)
        {
            if (term > _state.CurrentTerm)
                _state.UpdateTermAndVote(term, null);

            // The database is being rebuilt: answer before touching _db.
            if (_resyncing)
                return new AppendEntriesResult(_state.CurrentTerm, false, 0, "resyncing");

            if (term < _state.CurrentTerm)
                return new AppendEntriesResult(_state.CurrentTerm, false, _db.GetLatestSequenceNumber(), "stale term");

            BecomeFollower(leaderId);

            // Adopt cluster mode if the leader has promoted.
            if (leaderMode == ClusterMode.Cluster && _state.Mode == ClusterMode.Bootstrap)
                PromoteToClusterLocked();

            // log consistency check
            ulong myLatest = _db.GetLatestSequenceNumber();
            if (prevLogSeq > 0 && prevLogSeq > myLatest)
            {
                result = new AppendEntriesResult(_state.CurrentTerm, false, myLatest, "behind");
            }
            else if (prevLogSeq > 0 && _state.Terms.GetTermForSeq(prevLogSeq) != prevLogTerm)
            {
                // Our entry at prevLogSeq disagrees with the leader's. Raft
                // would truncate the conflicting suffix; a RocksDB WAL cannot
                // be truncated, so this replica must be rebuilt from a leader
                // snapshot.
                conflict = true;
                result = new AppendEntriesResult(_state.CurrentTerm, false, myLatest, "term-mismatch");
            }
            else
            {
                ulong newCommit = Math.Min(leaderCommit, myLatest);
                if (newCommit > _commitSeq)
                    Volatile.Write(ref _commitSeq, newCommit);
                result = new AppendEntriesResult(_state.CurrentTerm, true, myLatest, null);
            }
        }

        if (conflict)
            ResyncRequired?.Invoke(this, EventArgs.Empty);

        return result;
    }

    /// <summary>
    /// Followers call this when they observe a batch on the WAL stream (the
    /// existing replication stream is leader → follower, so the leader tags
    /// its current term and node id on every batch it streams).
    /// </summary>
    public void HandleObservedLeader(string leaderId, long term)
    {
        lock (_gate)
        {
            if (term < _state.CurrentTerm) return;
            // A leader can never be replaced within its own term; only a
            // strictly higher term may depose us.
            if (term == _state.CurrentTerm && _role == RaftRole.Leader) return;
            if (term > _state.CurrentTerm)
                _state.UpdateTermAndVote(term, null);
            BecomeFollower(leaderId);
        }
    }

    /// <summary>
    /// Leader-side half of the stream-attach consistency check. Before a
    /// follower starts consuming the WAL stream it sends its (latestSeq,
    /// termAtLatest) pair; the leader confirms that pair exists identically in
    /// its own log. A follower whose tail diverged (e.g. a deposed leader with
    /// uncommitted writes) is told to rebuild itself from a snapshot.
    /// </summary>
    public LogConsistencyResult CheckFollowerLogConsistency(ulong followerLatestSeq, long followerTermAtLatest)
    {
        lock (_gate)
        {
            if (_role != RaftRole.Leader || _resyncing)
                return new LogConsistencyResult(false, false, _state.CurrentTerm);

            if (followerLatestSeq == 0)
                return new LogConsistencyResult(true, true, _state.CurrentTerm);

            ulong myLatest = _db.GetLatestSequenceNumber();
            if (followerLatestSeq > myLatest)
            {
                // The follower has entries the leader does not: an uncommitted
                // tail from a previous term. It must resync.
                return new LogConsistencyResult(true, false, _state.CurrentTerm);
            }

            bool consistent = _state.Terms.GetTermForSeq(followerLatestSeq) == followerTermAtLatest;
            return new LogConsistencyResult(true, consistent, _state.CurrentTerm);
        }
    }

    // ===== Resync lifecycle (host-driven) =====

    /// <summary>
    /// Puts the node into resync mode: the election timer is paused and all
    /// vote / append handling answers conservatively without touching the
    /// database, so the host can safely dispose and rebuild it.
    /// </summary>
    public void BeginResync()
    {
        lock (_gate)
        {
            _resyncing = true;
            _electionTimer.Pause();
        }
    }

    /// <summary>
    /// Completes a resync with the freshly restored database instance.
    /// </summary>
    public void CompleteResync(RocksDb db)
    {
        lock (_gate)
        {
            _db = db ?? throw new ArgumentNullException(nameof(db));
            _resyncing = false;
            if (_role != RaftRole.Leader)
                _electionTimer.Reset();
        }
    }

    // ===== Leader-only: log replication advance =====

    public void RecordFollowerProgress(string nodeId, ulong matchSeq)
    {
        if (!_peers.TryGetValue(nodeId, out var peer)) return;
        lock (_gate)
        {
            if (_resyncing) return;
            peer.OnAppendSucceeded(matchSeq);
            TryAdvanceCommitLocked();
            MaybePromoteToClusterLocked();
        }
    }

    public void RecordFollowerHeartbeat(string nodeId)
    {
        if (_peers.TryGetValue(nodeId, out var peer)) peer.OnHeartbeatAcknowledged();
    }

    public void RecordFollowerUnreachable(string nodeId)
    {
        if (_peers.TryGetValue(nodeId, out var peer)) peer.OnAppendFailed();
    }

    private void TryAdvanceCommitLocked()
    {
        if (_role != RaftRole.Leader) return;
        // sort matchSeqs descending; quorum-th value is the new commitIndex
        var seqs = _peers.Values.Select(p => p.MatchSeq).ToList();
        seqs.Add(_db.GetLatestSequenceNumber()); // include self
        seqs.Sort((a, b) => b.CompareTo(a));
        int q = _config.Quorum;
        if (seqs.Count < q) return;
        ulong candidate = seqs[q - 1];
        if (candidate <= _commitSeq) return;
        // §5.4.2: a leader may only commit entries from its own term by
        // counting replicas; entries from earlier terms become committed
        // implicitly once a current-term entry is committed.
        if (_state.Terms.GetTermForSeq(candidate) != _state.CurrentTerm) return;
        Volatile.Write(ref _commitSeq, candidate);
    }

    private void MaybePromoteToClusterLocked()
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
            PromoteToClusterLocked();
        }
    }

    private void PromoteToClusterLocked()
    {
        if (_state.Mode == ClusterMode.Cluster) return;
        _state.SetMode(ClusterMode.Cluster);
        ModeChanged?.Invoke(this, new ModeChangedEventArgs(ClusterMode.Cluster));
    }

    // ===== State transitions =====

    private void BecomeFollower(string? leaderId)
    {
        bool roleChanged = _role != RaftRole.Follower;
        bool leaderChanged = _currentLeaderId != leaderId;
        _role = RaftRole.Follower;
        _currentLeaderId = leaderId;
        _leaderCts?.Cancel();
        _leaderCts = null;
        if (roleChanged) RoleChanged?.Invoke(this, new RoleChangedEventArgs(_role, _state.CurrentTerm));
        if (leaderChanged) LeaderChanged?.Invoke(this, new LeaderChangedEventArgs(leaderId, _state.CurrentTerm));
        if (!_resyncing) _electionTimer.Reset();
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
        // matchSeq must be reset to 0 on every election: values learned during
        // a previous leadership are stale and could falsely advance the commit
        // index before this term replicated anything.
        foreach (var p in _peers.Values) p.ResetForNewTerm(nextSeq);
        _state.RecordTermRange(nextSeq, _state.CurrentTerm);
        // Replicate one entry from the new term right away so the commit index
        // can advance past entries inherited from previous terms (§5.4.2).
        _db.Put(LeaderNoOpKey, Encoding.UTF8.GetBytes(_state.CurrentTerm.ToString()));
        RoleChanged?.Invoke(this, new RoleChangedEventArgs(_role, _state.CurrentTerm));
        LeaderChanged?.Invoke(this, new LeaderChangedEventArgs(_config.NodeId, _state.CurrentTerm));
        _electionTimer.Pause();
        StartLeaderLoop();
    }

    private void StartLeaderLoop()
    {
        // Cancel any loop left over from a previous leadership so two loops
        // never run concurrently.
        _leaderCts?.Cancel();
        _leaderCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        var token = _leaderCts.Token;
        _ = Task.Run(() => LeaderHeartbeatLoopAsync(token));
    }

    private async Task LeaderHeartbeatLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                lock (_gate) { if (_role != RaftRole.Leader) break; }

                ulong commit = Volatile.Read(ref _commitSeq);
                long term = _state.CurrentTerm;
                var leaderMode = _state.Mode;

                foreach (var peer in _peers.Values)
                {
                    ulong prevLogSeq = peer.NextSeq > 0 ? peer.NextSeq - 1 : 0;
                    long prevLogTerm = _state.Terms.GetTermForSeq(prevLogSeq);
                    var beat = new HeartbeatPayload(term, _config.NodeId, prevLogSeq, prevLogTerm, commit, leaderMode);
                    _ = SendHeartbeatToPeerAsync(peer, beat, ct);
                }

                await Task.Delay(_config.HeartbeatIntervalMs, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { }
    }

    private async Task SendHeartbeatToPeerAsync(PeerState peer, HeartbeatPayload beat, CancellationToken ct)
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
                    if (resp.Term > _state.CurrentTerm)
                    {
                        _state.UpdateTermAndVote(resp.Term, null);
                        BecomeFollower(null);
                    }
                }
                return;
            }
            if (resp.Success)
            {
                lock (_gate)
                {
                    // Only the prefix ending at PrevLogSeq has been validated
                    // against our log. Anything past it is confirmed by the
                    // WAL-stream progress reports, not by the heartbeat.
                    peer.OnAppendSucceeded(beat.PrevLogSeq);
                    TryAdvanceCommitLocked();
                    MaybePromoteToClusterLocked();
                }
            }
            else if (resp.Reason == "behind")
            {
                // The follower's log is shorter; probe at its tip next time.
                peer.RewindOnConflict(Math.Max(1, resp.LastSeq + 1));
            }
            else
            {
                // term-mismatch (the follower resyncs itself) or resyncing:
                // treat the peer as unreachable until it comes back.
                peer.OnAppendFailed();
            }
        }
        catch { peer.OnAppendFailed(); }
    }

    private void OnElectionTimeout()
    {
        long electionTerm;
        ulong lastLogSeq;
        long lastLogTerm;
        lock (_gate)
        {
            if (_role == RaftRole.Leader || _resyncing) return;
            if (_state.Mode == ClusterMode.Bootstrap)
            {
                // While in Bootstrap mode the cluster has not yet promoted; only
                // the bootstrap primary may be leader, so reset and wait.
                _electionTimer.Reset();
                return;
            }
            BecomeCandidate();
            // The campaign is bound to the exact term in which we became
            // candidate. Capturing it outside this lock would let a concurrent
            // vote or heartbeat move the term forward and make us campaign in
            // a term where we may already have voted for someone else.
            electionTerm = _state.CurrentTerm;
            lastLogSeq = _db.GetLatestSequenceNumber();
            lastLogTerm = _state.Terms.GetTermForSeq(lastLogSeq);
            _electionTimer.Reset();
        }

        _ = Task.Run(() => RunElectionAsync(electionTerm, lastLogSeq, lastLogTerm));
    }

    private async Task RunElectionAsync(long term, ulong lastLogSeq, long lastLogTerm)
    {
        List<ClusterMember> peers;
        lock (_gate)
        {
            if (_role != RaftRole.Candidate || _state.CurrentTerm != term) return;
            peers = _peers.Values.Select(p => p.Member).ToList();
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
                    if (resp.Term > _state.CurrentTerm)
                    {
                        _state.UpdateTermAndVote(resp.Term, null);
                        BecomeFollower(null);
                    }
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
public sealed record LogConsistencyResult(bool IsLeader, bool Consistent, long Term);

public interface IPeerTransport
{
    Task<RequestVoteResult?> SendRequestVoteAsync(ClusterMember peer, long term, string candidateId, ulong lastLogSeq, long lastLogTerm, CancellationToken ct);
    Task<AppendEntriesResult?> SendHeartbeatAsync(ClusterMember peer, HeartbeatPayload payload, CancellationToken ct);
}
#endif
