using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using MagicOnion.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RocksDbSharp;

namespace ClusterTest;

/// <summary>
/// Per-process host. Owns the RocksDB instance, the RaftClusterNode, the
/// MagicOnion server, and (when this node is a follower) the WAL-streaming
/// background task that ingests batches from the current leader.
/// </summary>
public sealed class ClusterNodeHost : IAsyncDisposable
{
    public RocksDb Db { get; private set; } = null!;
    public RaftClusterNode Node { get; private set; } = null!;

    public bool IsResyncing => _resyncing;
    public bool WriteLoadEnabled
    {
        get => _writeLoadEnabled;
        set => _writeLoadEnabled = value;
    }

    private readonly NodeConfiguration _cfg;
    private readonly ClusterPeerTransport _transport = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly SemaphoreSlim _resyncGate = new(1, 1);

    private WebApplication? _app;
    private Task? _appRunTask;
    private Task? _followerStreamTask;
    private string? _currentFollowingLeader;
    private CancellationTokenSource? _followerCts;
    private RaftPersistentState? _state;
    private volatile bool _resyncing;
    private volatile bool _writeLoadEnabled = true;

    public ClusterNodeHost(NodeConfiguration cfg)
    {
        _cfg = cfg;
    }

    public async Task StartAsync()
    {
        Directory.CreateDirectory(_cfg.DbPath);
        var raftDir = Path.Combine(_cfg.DbPath, "raft");
        Directory.CreateDirectory(raftDir);

        _state = new RaftPersistentState(raftDir);

        // Non-bootstrap nodes: pull initial snapshot from primary only when the
        // DB is fresh. CURRENT is a *file*; testing it with Directory.Exists
        // made every restart wrongly re-pull a full snapshot over live data.
        if (!_cfg.BootstrapPrimary && !File.Exists(Path.Combine(_cfg.DbPath, "CURRENT")))
        {
            await SyncInitialFromPrimaryAsync();
        }

        OpenDatabase();

        var peers = _cfg.AllMembers.Where(m => m.NodeId != _cfg.NodeId)
                                   .Select(m => new ClusterMember(m.NodeId, m.Endpoint))
                                   .ToList();

        var raftCfg = new RaftConfig(
            nodeId: _cfg.NodeId,
            endpoint: _cfg.Endpoint,
            peers: peers,
            bootstrapPrimary: _cfg.BootstrapPrimary,
            electionTimeoutMinMs: _cfg.ElectionTimeoutMinMs,
            electionTimeoutMaxMs: _cfg.ElectionTimeoutMaxMs,
            heartbeatIntervalMs: _cfg.HeartbeatIntervalMs,
            bootstrapInSyncLagThreshold: _cfg.BootstrapInSyncLagThreshold);

        Node = new RaftClusterNode(raftCfg, _state, Db, _transport);

        Node.LeaderChanged += OnLeaderChanged;
        Node.RoleChanged += (s, e) =>
            Log($"role -> {e.Role} (term {e.Term})");
        Node.ModeChanged += (s, e) =>
            Log($"mode -> {e.Mode}");
        Node.ResyncRequired += (s, e) => TriggerResync();

        await StartServerAsync();

        Node.Start();

        // If we are a non-bootstrap follower, start streaming from the bootstrap primary immediately.
        if (!_cfg.BootstrapPrimary)
        {
            var primary = _cfg.AllMembers.FirstOrDefault(m => m.IsBootstrapPrimary);
            if (primary != null && primary.NodeId != _cfg.NodeId)
            {
                EnsureFollowerStream(primary.NodeId, primary.Endpoint);
            }
        }
    }

    private void OpenDatabase()
    {
        var walDir = Path.Combine(_cfg.DbPath, "journal");
        var options = new DbOptions()
            .SetCreateIfMissing(true)
            .SetWalDir(walDir)
            .SetWalTtlSeconds(120)
            .SetMaxTotalWalSize(1024UL * 1024 * 32)
            .SetWalSizeLimitMB(1024UL * 1024 * 4);
        Db = RocksDb.Open(options, _cfg.DbPath);
        Db.DisableFileDeletions();
    }

    // -----------------------------------------------------------------------
    // Write helpers (leader-side)
    // -----------------------------------------------------------------------

    /// <summary>
    /// Fire-and-forget write used by the load generator. Returns false when
    /// this node is not the leader (or is rebuilding itself) so the generator
    /// stops immediately after a step-down instead of polluting the local WAL.
    /// </summary>
    public bool TryWriteAsLeader(string key, string value)
    {
        if (_resyncing) return false;
        if (Node is not { Role: RaftRole.Leader }) return false;
        Db.Put(key, value);
        return true;
    }

    /// <summary>
    /// Write acknowledged only after the entry is committed (replicated to a
    /// quorum). This is the durability contract Raft offers to clients.
    /// </summary>
    public async Task<bool> WriteCommittedAsync(string key, string value, TimeSpan timeout)
    {
        if (_resyncing || Node.Role != RaftRole.Leader) return false;
        Db.Put(key, value);
        ulong seq = Db.GetLatestSequenceNumber();
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            if (Node.CommitSeq >= seq) return true;
            if (Node.Role != RaftRole.Leader) return false;
            await Task.Delay(15);
        }
        return false;
    }

    public string SerializeTerms() => _state!.Terms.Serialize();

    // -----------------------------------------------------------------------
    // Follower stream
    // -----------------------------------------------------------------------

    private void OnLeaderChanged(object? sender, LeaderChangedEventArgs e)
    {
        Log($"leader -> {e.LeaderId ?? "(none)"} term {e.Term}");
        if (e.LeaderId == null || e.LeaderId == _cfg.NodeId)
        {
            CancelFollowerStream();
            return;
        }
        var leader = _cfg.AllMembers.FirstOrDefault(m => m.NodeId == e.LeaderId);
        if (leader != null) EnsureFollowerStream(leader.NodeId, leader.Endpoint);
    }

    private void CancelFollowerStream()
    {
        var c = _followerCts;
        _followerCts = null;
        _currentFollowingLeader = null;
        try { c?.Cancel(); } catch { }
    }

    private void EnsureFollowerStream(string leaderId, string leaderEndpoint)
    {
        if (_resyncing) return; // the resync flow re-attaches when it finishes
        if (_currentFollowingLeader == leaderId && _followerStreamTask is { IsCompleted: false }) return;
        CancelFollowerStream();
        _followerCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        _currentFollowingLeader = leaderId;
        var ct = _followerCts.Token;
        _followerStreamTask = Task.Run(() => RunFollowerStreamAsync(leaderId, leaderEndpoint, ct));
    }

    private async Task RunFollowerStreamAsync(string leaderId, string endpoint, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            GrpcChannel? ch = null;
            CancellationTokenSource? connCts = null;
            try
            {
                ch = GrpcChannel.ForAddress(endpoint);
                var client = MagicOnionClient.Create<IClusterService>(ch);

                // Raft's AppendEntries consistency check, done once at attach
                // time: the leader confirms that our (latestSeq, term) tail
                // exists identically in its log. A diverged tail (uncommitted
                // writes from a deposed leadership) cannot be truncated out of
                // a RocksDB WAL, so the whole replica is rebuilt instead.
                ulong myLatest = Db.GetLatestSequenceNumber();
                var check = await client.CheckLogConsistencyAsync(new LogConsistencyRequest
                {
                    NodeId = _cfg.NodeId,
                    FollowerLatestSeq = myLatest,
                    FollowerTermAtLatest = Node.GetTermForSeq(myLatest),
                });
                if (!check.IsLeader)
                {
                    await Task.Delay(500, ct);
                    continue;
                }
                if (!check.Consistent)
                {
                    Log($"log diverged from leader {leaderId}; scheduling full resync");
                    TriggerResync();
                    return;
                }

                ulong startSeq = myLatest + 1;
                Log($"following {leaderId} from seq {startSeq:n0}");
                var stream = await client.SyncUpdatesAsync(new SyncUpdatesRequest
                {
                    NodeId = _cfg.NodeId,
                    StartSeq = startSeq,
                });

                connCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                var reporterCt = connCts.Token;
                // Report progress on a timer rather than every N batches so the
                // leader's matchSeq (and therefore the commit index) keeps
                // advancing even when the write load pauses.
                _ = Task.Run(async () =>
                {
                    try
                    {
                        using var rch = GrpcChannel.ForAddress(endpoint);
                        var rclient = MagicOnionClient.Create<IClusterService>(rch);
                        while (!reporterCt.IsCancellationRequested)
                        {
                            if (!_resyncing)
                            {
                                try { await rclient.ReportLastSyncSequenceNumber(_cfg.NodeId, Db.GetLatestSequenceNumber()); }
                                catch { }
                            }
                            await Task.Delay(200, reporterCt);
                        }
                    }
                    catch (OperationCanceledException) { }
                }, reporterCt);

                var consumer = new ReplicationConsumer(Db);
                while (await stream.ResponseStream.MoveNext(ct))
                {
                    var batch = stream.ResponseStream.Current;

                    // A faithful replica ingests the leader's batches at
                    // exactly the same sequence numbers. Any gap means the
                    // leader truncated WAL past our position; any overlap
                    // means a local write slipped in. Both diverge the copy.
                    ulong expected = Db.GetLatestSequenceNumber() + 1;
                    if (batch.SequenceNumber != expected)
                    {
                        Log($"WAL stream mismatch: expected seq {expected:n0}, got {batch.SequenceNumber:n0}; scheduling full resync");
                        batch.ReturnToPool();
                        TriggerResync();
                        return;
                    }

                    Node.HandleObservedLeader(batch.LeaderId, batch.LeaderTerm);
                    if (batch.EntryTerm > 0)
                        _state!.RecordTermRange(batch.SequenceNumber, batch.EntryTerm);
                    consumer.IngestBatch(batch.SequenceNumber, batch.Data);
                    batch.ReturnToPool();
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                Log($"follower stream error: {ex.Message}; retrying in 1s");
                try { await Task.Delay(1000, ct); } catch { }
            }
            finally
            {
                connCts?.Cancel();
                connCts?.Dispose();
                ch?.Dispose();
            }
        }
    }

    // -----------------------------------------------------------------------
    // Snapshot pull + full resync
    // -----------------------------------------------------------------------

    private async Task SyncInitialFromPrimaryAsync()
    {
        var primary = _cfg.AllMembers.FirstOrDefault(m => m.IsBootstrapPrimary);
        if (primary == null) throw new InvalidOperationException("No bootstrap primary in configuration");
        if (primary.NodeId == _cfg.NodeId) return;

        Log($"snapshot sync from {primary.NodeId} @ {primary.Endpoint}");

        // primary may not be up yet; retry
        Exception? last = null;
        for (int attempt = 0; attempt < 30; attempt++)
        {
            try
            {
                await PullSnapshotAsync(primary.Endpoint);
                return;
            }
            catch (Exception ex)
            {
                last = ex;
                await Task.Delay(1000);
            }
        }
        throw new Exception("snapshot sync failed", last);
    }

    private async Task PullSnapshotAsync(string endpoint)
    {
        using var ch = GrpcChannel.ForAddress(endpoint);
        var client = MagicOnionClient.Create<IClusterService>(ch);
        var stream = await client.SyncInitialStateAsync();
        Directory.CreateDirectory(_cfg.DbPath);

        FileStream? current = null;
        string? currentName = null;
        try
        {
            while (await stream.ResponseStream.MoveNext(_cts.Token))
            {
                var chunk = stream.ResponseStream.Current;
                if (currentName != chunk.FileName)
                {
                    current?.Dispose();
                    currentName = chunk.FileName;
                    current = new FileStream(Path.Combine(_cfg.DbPath, chunk.FileName), FileMode.Create, FileAccess.Write);
                }
                if (chunk.Content.Length > 0)
                    await current!.WriteAsync(chunk.Content);
            }
        }
        finally
        {
            current?.Dispose();
        }

        // The restored log is byte-for-byte the leader's log, so the leader's
        // term map is the correct description of it.
        var terms = await client.GetRaftTermsAsync();
        _state!.ReplaceTerms(RaftLogTerms.Deserialize(terms));
        Log("snapshot sync complete");
    }

    public void TriggerResync()
    {
        if (_resyncing || _cts.IsCancellationRequested) return;
        _ = Task.Run(PerformResyncAsync);
    }

    private async Task PerformResyncAsync()
    {
        if (!await _resyncGate.WaitAsync(0)) return;
        try
        {
            if (_cts.IsCancellationRequested) return;
            _resyncing = true;
            Node.BeginResync();
            CancelFollowerStream();
            Log("resync: local log conflicts with the leader's; rebuilding replica from a leader snapshot");

            // Give in-flight RPC handlers a moment to finish touching the DB
            // before it is disposed.
            await Task.Delay(1000);
            Db.Dispose();

            while (!_cts.IsCancellationRequested)
            {
                try
                {
                    var leader = await FindLeaderAsync();
                    if (leader == null)
                    {
                        await Task.Delay(1000);
                        continue;
                    }
                    WipeDataDirectory();
                    await PullSnapshotAsync(leader.Endpoint);
                    OpenDatabase();
                    Node.CompleteResync(Db);
                    _resyncing = false;
                    Log($"resync complete at seq {Db.GetLatestSequenceNumber():n0}; rejoining {leader.NodeId}");
                    EnsureFollowerStream(leader.NodeId, leader.Endpoint);
                    return;
                }
                catch (Exception ex)
                {
                    Log($"resync attempt failed: {ex.Message}; retrying in 1s");
                    await Task.Delay(1000);
                }
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            _resyncGate.Release();
        }
    }

    private async Task<MemberConfig?> FindLeaderAsync()
    {
        foreach (var m in _cfg.AllMembers.Where(m => m.NodeId != _cfg.NodeId))
        {
            try
            {
                using var ch = GrpcChannel.ForAddress(m.Endpoint);
                var client = MagicOnionClient.Create<IClusterService>(ch);
                var s = await client.GetStatusAsync();
                if (s.Role == nameof(RaftRole.Leader)) return m;
                if (!string.IsNullOrEmpty(s.LeaderId) && s.LeaderId != _cfg.NodeId)
                {
                    var l = _cfg.AllMembers.FirstOrDefault(x => x.NodeId == s.LeaderId);
                    if (l != null) return l;
                }
            }
            catch { }
        }
        return null;
    }

    private void WipeDataDirectory()
    {
        foreach (var dir in Directory.GetDirectories(_cfg.DbPath))
        {
            if (Path.GetFileName(dir).Equals("raft", StringComparison.OrdinalIgnoreCase)) continue;
            Directory.Delete(dir, true);
        }
        foreach (var file in Directory.GetFiles(_cfg.DbPath))
        {
            if (Path.GetFileName(file).Equals("node.json", StringComparison.OrdinalIgnoreCase)) continue;
            File.Delete(file);
        }
    }

    // -----------------------------------------------------------------------

    private async Task StartServerAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddGrpc();
        builder.Services.AddMagicOnion();
        builder.Services.AddSingleton(this);
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.WebHost.ConfigureKestrel(o =>
        {
            o.ListenLocalhost(_cfg.Port, lo => lo.Protocols = HttpProtocols.Http2);
        });
        _app = builder.Build();
        _app.MapMagicOnionService();
        _appRunTask = _app.RunAsync(_cts.Token);
        await Task.Delay(300); // let the listener bind
    }

    public List<ClusterMemberInfo> GetMemberInfos() =>
        _cfg.AllMembers.Select(m => new ClusterMemberInfo { NodeId = m.NodeId, Endpoint = m.Endpoint }).ToList();

    public void OnFollowerProgress(string nodeId, ulong seq)
    {
        // hook for the host - kept for symmetry with the existing replication module
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try { if (_app != null) await _app.StopAsync(); } catch { }
        try { if (_appRunTask != null) await _appRunTask; } catch { }
        _transport.Dispose();
        Node?.Dispose();
        if (!_resyncing) Db?.Dispose();
    }

    private void Log(string msg) =>
        Console.WriteLine($"[{DateTimeOffset.UtcNow:HH:mm:ss.fff} {_cfg.NodeId}] {msg}");
}

public sealed class NodeConfiguration
{
    public string NodeId { get; set; } = "";
    public string Endpoint { get; set; } = "";
    public int Port { get; set; }
    public string DbPath { get; set; } = "";
    public bool BootstrapPrimary { get; set; }
    public List<MemberConfig> AllMembers { get; set; } = new();
    public int ElectionTimeoutMinMs { get; set; } = 1500;
    public int ElectionTimeoutMaxMs { get; set; } = 3000;
    public int HeartbeatIntervalMs { get; set; } = 300;
    public ulong BootstrapInSyncLagThreshold { get; set; } = 1000;
}

public sealed class MemberConfig
{
    public string NodeId { get; set; } = "";
    public string Endpoint { get; set; } = "";
    public bool IsBootstrapPrimary { get; set; }
}
