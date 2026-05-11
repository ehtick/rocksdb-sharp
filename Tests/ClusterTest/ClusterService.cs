using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using MagicOnion;
using MagicOnion.Server;
using RocksDbSharp;

namespace ClusterTest;

public class ClusterService : ServiceBase<IClusterService>, IClusterService
{
    private readonly ClusterNodeHost _host;

    public ClusterService(ClusterNodeHost host)
    {
        _host = host;
    }

    public UnaryResult<RequestVoteResponse> RequestVoteAsync(RequestVoteRequest req)
    {
        var r = _host.Node.HandleRequestVote(req.Term, req.CandidateId, req.LastLogSeq, req.LastLogTerm);
        return new UnaryResult<RequestVoteResponse>(new RequestVoteResponse
        {
            Term = r.Term,
            VoteGranted = r.VoteGranted,
        });
    }

    public UnaryResult<AppendEntriesResponse> HeartbeatAsync(HeartbeatRequest req)
    {
        var r = _host.Node.HandleAppendEntries(
            req.Term, req.LeaderId, req.PrevLogSeq, req.PrevLogTerm, req.LeaderCommit, (ClusterMode)req.LeaderMode);
        return new UnaryResult<AppendEntriesResponse>(new AppendEntriesResponse
        {
            Term = r.Term,
            Success = r.Success,
            LastSeq = r.LastSeq,
            Reason = r.Reason,
        });
    }

    public UnaryResult<JoinResponse> JoinAsync(JoinRequest req)
    {
        var resp = new JoinResponse
        {
            Accepted = true,
            LeaderId = _host.Node.CurrentLeaderId ?? "",
            Term = _host.Node.CurrentTerm,
            Mode = (int)_host.Node.Mode,
            Members = _host.GetMemberInfos(),
        };
        return new UnaryResult<JoinResponse>(resp);
    }

    public UnaryResult<NodeStatus> GetStatusAsync()
    {
        var s = new NodeStatus
        {
            NodeId = _host.Node.NodeId,
            Role = _host.Node.Role.ToString(),
            Term = _host.Node.CurrentTerm,
            LeaderId = _host.Node.CurrentLeaderId,
            LatestSeq = _host.Db.GetLatestSequenceNumber(),
            CommitSeq = _host.Node.CommitSeq,
            Mode = (int)_host.Node.Mode,
            BootstrapInSyncCount = _host.Node.BootstrapInSyncCount,
            Peers = _host.Node.Peers.Select(p => new PeerInfo
            {
                NodeId = p.Member.NodeId,
                MatchSeq = p.MatchSeq,
                Reachable = p.IsReachable,
            }).ToList(),
        };
        return new UnaryResult<NodeStatus>(s);
    }

    public async Task<ServerStreamingResult<ClusterFileData>> SyncInitialStateAsync()
    {
        var stream = GetServerStreamingContext<ClusterFileData>();
        var src = new ReplicationSource(_host.Db);
        var tempPath = Path.Combine(Path.GetTempPath(), "rocksdb_cluster_snap_" + Guid.NewGuid().ToString());
        using (var session = src.GetInitialState(tempPath))
        {
            foreach (var file in session.Files)
            {
                using var ms = new MemoryStream();
                await file.FileStream.CopyToAsync(ms);
                await stream.WriteAsync(new ClusterFileData
                {
                    FileName = file.FileName,
                    FileSize = file.FileSize,
                    Content = ms.ToArray(),
                });
                file.Dispose();
            }
        }
        return stream.Result();
    }

    public async Task<ServerStreamingResult<ClusterBatchData>> SyncUpdatesAsync(SyncUpdatesRequest req)
    {
        var stream = GetServerStreamingContext<ClusterBatchData>();
        var src = new ReplicationSource(_host.Db);

        ulong currentSeq = req.StartSeq;
        var ct = Context.CallContext.CancellationToken;

        while (!ct.IsCancellationRequested)
        {
            bool any = false;
            ulong latest = _host.Db.GetLatestSequenceNumber();
            if (currentSeq <= latest)
            {
                foreach (var batch in src.GetPooledWalUpdates(currentSeq))
                {
                    any = true;
                    long term = _host.Node.CurrentTerm;
                    await stream.WriteAsync(new ClusterBatchData
                    {
                        SequenceNumber = batch.SequenceNumber,
                        Length = batch.Length,
                        PooledData = batch.PooledData,
                        LeaderTerm = term,
                        LeaderId = _host.Node.NodeId,
                    });
                    currentSeq = batch.SequenceNumber + 1;
                    WriteBatch.ReturnPooledBytes(batch.PooledData);
                }
            }
            if (!any)
            {
                try { await Task.Delay(50, ct); }
                catch (OperationCanceledException) { break; }
            }
        }
        return stream.Result();
    }

    public UnaryResult<bool> ReportLastSyncSequenceNumber(string nodeId, ulong seqNumber)
    {
        _host.Node.RecordFollowerProgress(nodeId, seqNumber);
        _host.OnFollowerProgress(nodeId, seqNumber);
        return new UnaryResult<bool>(true);
    }

    public UnaryResult<bool> WriteAsync(WriteRequest req)
    {
        // Reject writes that arrive at non-leaders. Clients can introspect with
        // GetStatusAsync to find the leader.
        if (_host.Node.Role != RaftRole.Leader)
            return new UnaryResult<bool>(false);
        _host.Db.Put(req.Key, req.Value);
        return new UnaryResult<bool>(true);
    }
}
