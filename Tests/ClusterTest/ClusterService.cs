using System;
using System.Diagnostics;
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
        bool resyncing = _host.IsResyncing;
        var s = new NodeStatus
        {
            NodeId = _host.Node.NodeId,
            Role = _host.Node.Role.ToString(),
            Term = _host.Node.CurrentTerm,
            LeaderId = _host.Node.CurrentLeaderId,
            LatestSeq = resyncing ? 0 : _host.Db.GetLatestSequenceNumber(),
            CommitSeq = _host.Node.CommitSeq,
            Mode = (int)_host.Node.Mode,
            BootstrapInSyncCount = _host.Node.BootstrapInSyncCount,
            Resyncing = resyncing,
            Peers = _host.Node.Peers.Select(p => new PeerInfo
            {
                NodeId = p.Member.NodeId,
                MatchSeq = p.MatchSeq,
                Reachable = p.IsReachable,
            }).ToList(),
        };
        return new UnaryResult<NodeStatus>(s);
    }

    /// <summary>
    /// Hash-request half of the delta transfer: hashes are computed only for
    /// files this node holds with the exact requested name and size. SSTs are
    /// immutable, so hashing the live file is equivalent to hashing the copy a
    /// later checkpoint would hard-link.
    /// </summary>
    public UnaryResult<List<FileHashResult>> GetFileHashesAsync(List<FileHashQuery> files)
    {
        var result = new List<FileHashResult>(files.Count);
        bool resyncing = _host.IsResyncing;
        foreach (var f in files)
        {
            string? hash = resyncing ? null : ReplicationDelta.TryHashCandidate(_host.DbPath, f.Name, f.Size);
            result.Add(new FileHashResult { Name = f.Name, Found = hash != null, Hash = hash ?? "" });
        }
        return new UnaryResult<List<FileHashResult>>(result);
    }

    public async Task<ServerStreamingResult<ClusterFileData>> SyncInitialStateAsync(SnapshotRequest req)
    {
        var stream = GetServerStreamingContext<ClusterFileData>();
        if (_host.IsResyncing) return stream.Result();

        var src = new ReplicationSource(_host.Db);
        var tempPath = Path.Combine(Path.GetTempPath(), "rocksdb_cluster_snap_" + Guid.NewGuid().ToString());
        using (var session = src.GetInitialState(tempPath))
        {
            // Per-file delta: the consumer lists immutable files it verified
            // through GetFileHashesAsync; those are reused on a name + size
            // match, everything new or changed is streamed in full.
            var plan = ReplicationDelta.Compute(
                session.GetManifest(),
                req.Files.Select(f => new ReplicationFileInfo { FileName = f.Name, Size = f.Size, Hash = string.Empty }));

            await stream.WriteAsync(new ClusterFileData
            {
                IsPlan = true,
                KeepFiles = plan.FilesToReuse,
            });

            // CURRENT names the live MANIFEST, so send it last: a consumer
            // that dies mid-transfer is left without a CURRENT marker and
            // will cleanly re-pull instead of opening a half-restored DB.
            var ordered = plan.FilesToTransfer.OrderBy(f => f == "CURRENT" ? 1 : 0).ToList();

            // Chunked so one message stays well below the 4 MB gRPC cap no
            // matter how large the checkpoint SSTs are.
            const int ChunkSize = 1024 * 1024;
            var buffer = new byte[ChunkSize];
            foreach (var name in ordered)
            {
                using (var file = session.OpenFile(name))
                {
                    bool sentAny = false;
                    int read;
                    while ((read = await file.FileStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                    {
                        await stream.WriteAsync(new ClusterFileData
                        {
                            FileName = file.FileName,
                            FileSize = file.FileSize,
                            Content = buffer.AsSpan(0, read).ToArray(),
                        });
                        sentAny = true;
                    }
                    if (!sentAny)
                    {
                        await stream.WriteAsync(new ClusterFileData
                        {
                            FileName = file.FileName,
                            FileSize = 0,
                            Content = Array.Empty<byte>(),
                        });
                    }
                }
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
            if (_host.IsResyncing) break;

            // Read the term before the role: if the role still says Leader
            // afterwards, the term we read belongs to this leadership. A node
            // that streamed while deposed would otherwise tag batches with the
            // *new* term while claiming to be the leader, convincing followers
            // a stale node still leads the cluster.
            long term = _host.Node.CurrentTerm;
            if (_host.Node.Role != RaftRole.Leader) break;

            bool any = false;
            ulong latest = _host.Db.GetLatestSequenceNumber();
            if (currentSeq <= latest)
            {
                foreach (var batch in src.GetPooledWalUpdates(currentSeq))
                {
                    any = true;
                    await stream.WriteAsync(new ClusterBatchData
                    {
                        SequenceNumber = batch.SequenceNumber,
                        Length = batch.Length,
                        PooledData = batch.PooledData,
                        LeaderTerm = term,
                        EntryTerm = _host.Node.GetTermForSeq(batch.SequenceNumber),
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

    public UnaryResult<LogConsistencyResponse> CheckLogConsistencyAsync(LogConsistencyRequest req)
    {
        var r = _host.Node.CheckFollowerLogConsistency(req.FollowerLatestSeq, req.FollowerTermAtLatest);
        return new UnaryResult<LogConsistencyResponse>(new LogConsistencyResponse
        {
            IsLeader = r.IsLeader,
            Consistent = r.Consistent,
            Term = r.Term,
        });
    }

    public UnaryResult<string> GetRaftTermsAsync()
    {
        return new UnaryResult<string>(_host.SerializeTerms());
    }

    public async UnaryResult<bool> WriteAsync(WriteRequest req)
    {
        // Acknowledge only once the write is replicated to a quorum and
        // committed; an immediate ack would let "committed" writes vanish in
        // a leader failover, which is precisely what Raft must not allow.
        return await _host.WriteCommittedAsync(req.Key, req.Value, TimeSpan.FromSeconds(10));
    }

    public UnaryResult<ReadResponse> ReadAsync(string key)
    {
        if (_host.IsResyncing) return new UnaryResult<ReadResponse>(new ReadResponse());
        var value = _host.Db.Get(key);
        return new UnaryResult<ReadResponse>(new ReadResponse { Found = value != null, Value = value });
    }

    public UnaryResult<ChecksumResponse> ChecksumAsync()
    {
        if (_host.IsResyncing) return new UnaryResult<ChecksumResponse>(new ChecksumResponse { Available = false });

        // Order-sensitive FNV-1a over every key/value pair. RocksDB iterates
        // in key order, so converged replicas must produce identical hashes.
        const ulong FnvOffset = 14695981039346656037UL;
        const ulong FnvPrime = 1099511628211UL;
        ulong hash = FnvOffset;
        long count = 0;
        using (var it = _host.Db.NewIterator())
        {
            for (it.SeekToFirst(); it.Valid(); it.Next())
            {
                foreach (var b in it.Key()) { hash ^= b; hash *= FnvPrime; }
                hash ^= 0xFF; hash *= FnvPrime;
                foreach (var b in it.Value()) { hash ^= b; hash *= FnvPrime; }
                hash ^= 0xFE; hash *= FnvPrime;
                count++;
            }
        }
        return new UnaryResult<ChecksumResponse>(new ChecksumResponse
        {
            Available = true,
            KeyCount = count,
            Hash = hash,
            LatestSeq = _host.Db.GetLatestSequenceNumber(),
        });
    }

    public UnaryResult<bool> SetWriteLoadAsync(bool enabled)
    {
        _host.WriteLoadEnabled = enabled;
        return new UnaryResult<bool>(true);
    }
}
