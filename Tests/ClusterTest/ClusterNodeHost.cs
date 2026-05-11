using System;
using System.Collections.Generic;
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

    private readonly NodeConfiguration _cfg;
    private readonly ClusterPeerTransport _transport = new();
    private readonly CancellationTokenSource _cts = new();

    private WebApplication? _app;
    private Task? _appRunTask;
    private Task? _followerStreamTask;
    private string? _currentFollowingLeader;
    private CancellationTokenSource? _followerCts;
    private RaftPersistentState? _state;

    public ClusterNodeHost(NodeConfiguration cfg)
    {
        _cfg = cfg;
    }

    public async Task StartAsync()
    {
        Directory.CreateDirectory(_cfg.DbPath);
        var walDir = Path.Combine(_cfg.DbPath, "journal");
        var raftDir = Path.Combine(_cfg.DbPath, "raft");
        Directory.CreateDirectory(raftDir);

        _state = new RaftPersistentState(raftDir);

        // Non-bootstrap nodes: pull initial snapshot from primary if their DB is fresh.
        if (!_cfg.BootstrapPrimary && !Directory.Exists(Path.Combine(_cfg.DbPath, "CURRENT")))
        {
            await SyncInitialFromPrimaryAsync();
        }

        var options = new DbOptions()
            .SetCreateIfMissing(true)
            .SetWalDir(walDir)
            .SetWalTtlSeconds(120)
            .SetMaxTotalWalSize(1024UL * 1024 * 32)
            .SetWalSizeLimitMB(1024UL * 1024 * 4);
        Db = RocksDb.Open(options, _cfg.DbPath);
        Db.DisableFileDeletions();

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
            try
            {
                var ch = GrpcChannel.ForAddress(endpoint);
                var client = MagicOnionClient.Create<IClusterService>(ch);
                ulong startSeq = Db.GetLatestSequenceNumber() + 1;
                Log($"following {leaderId} from seq {startSeq:n0}");
                var stream = await client.SyncUpdatesAsync(new SyncUpdatesRequest
                {
                    NodeId = _cfg.NodeId,
                    StartSeq = startSeq,
                });

                var consumer = new ReplicationConsumer(Db);
                int n = 0;
                while (await stream.ResponseStream.MoveNext(ct))
                {
                    var batch = stream.ResponseStream.Current;
                    Node.HandleObservedLeader(batch.LeaderId, batch.LeaderTerm);
                    // record term -> seq range as we observe new terms
                    var lastTerm = _state!.Terms.GetLast();
                    if (batch.LeaderTerm != lastTerm.Term)
                        _state.RecordTermRange(batch.SequenceNumber, batch.LeaderTerm);
                    consumer.IngestBatch(batch.SequenceNumber, batch.Data);
                    batch.ReturnToPool();
                    n++;
                    if (n % 500 == 0)
                    {
                        await client.ReportLastSyncSequenceNumber(_cfg.NodeId, Db.GetLatestSequenceNumber());
                    }
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                Log($"follower stream error: {ex.Message}; retrying in 1s");
                try { await Task.Delay(1000, ct); } catch { }
            }
        }
    }

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
                var ch = GrpcChannel.ForAddress(primary.Endpoint);
                var client = MagicOnionClient.Create<IClusterService>(ch);
                var stream = await client.SyncInitialStateAsync();
                Directory.CreateDirectory(_cfg.DbPath);
                while (await stream.ResponseStream.MoveNext(CancellationToken.None))
                {
                    var file = stream.ResponseStream.Current;
                    var dest = Path.Combine(_cfg.DbPath, file.FileName);
                    await File.WriteAllBytesAsync(dest, file.Content);
                }
                Log("snapshot sync complete");
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
        Db?.Dispose();
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
