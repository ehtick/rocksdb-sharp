using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading.Tasks;
using Grpc.Core;
using MagicOnion;
using MagicOnion.Serialization;
using MessagePack;
using MessagePack.Formatters;

namespace ClusterTest;

public interface IClusterService : IService<IClusterService>
{
    // ---- Raft RPCs ----
    UnaryResult<RequestVoteResponse> RequestVoteAsync(RequestVoteRequest req);
    UnaryResult<AppendEntriesResponse> HeartbeatAsync(HeartbeatRequest req);

    // ---- Cluster join / status ----
    UnaryResult<JoinResponse> JoinAsync(JoinRequest req);
    UnaryResult<NodeStatus> GetStatusAsync();

    // ---- Snapshot + WAL stream (reused from replication path) ----
    Task<ServerStreamingResult<ClusterFileData>> SyncInitialStateAsync();
    Task<ServerStreamingResult<ClusterBatchData>> SyncUpdatesAsync(SyncUpdatesRequest req);
    UnaryResult<bool> ReportLastSyncSequenceNumber(string nodeId, ulong seqNumber);

    // ---- Log consistency / resync ----
    UnaryResult<LogConsistencyResponse> CheckLogConsistencyAsync(LogConsistencyRequest req);
    UnaryResult<string> GetRaftTermsAsync();

    // ---- Testing helpers ----
    UnaryResult<bool> WriteAsync(WriteRequest req);
    UnaryResult<ReadResponse> ReadAsync(string key);
    UnaryResult<ChecksumResponse> ChecksumAsync();
    UnaryResult<bool> SetWriteLoadAsync(bool enabled);
}

// =============== DTOs ===============

[MessagePackObject]
public class RequestVoteRequest
{
    [Key(0)] public long Term { get; set; }
    [Key(1)] public string CandidateId { get; set; } = "";
    [Key(2)] public ulong LastLogSeq { get; set; }
    [Key(3)] public long LastLogTerm { get; set; }
}

[MessagePackObject]
public class RequestVoteResponse
{
    [Key(0)] public long Term { get; set; }
    [Key(1)] public bool VoteGranted { get; set; }
}

[MessagePackObject]
public class HeartbeatRequest
{
    [Key(0)] public long Term { get; set; }
    [Key(1)] public string LeaderId { get; set; } = "";
    [Key(2)] public ulong PrevLogSeq { get; set; }
    [Key(3)] public long PrevLogTerm { get; set; }
    [Key(4)] public ulong LeaderCommit { get; set; }
    [Key(5)] public int LeaderMode { get; set; }
}

[MessagePackObject]
public class AppendEntriesResponse
{
    [Key(0)] public long Term { get; set; }
    [Key(1)] public bool Success { get; set; }
    [Key(2)] public ulong LastSeq { get; set; }
    [Key(3)] public string? Reason { get; set; }
}

[MessagePackObject]
public class JoinRequest
{
    [Key(0)] public string NodeId { get; set; } = "";
    [Key(1)] public string Endpoint { get; set; } = "";
}

[MessagePackObject]
public class JoinResponse
{
    [Key(0)] public bool Accepted { get; set; }
    [Key(1)] public string LeaderId { get; set; } = "";
    [Key(2)] public long Term { get; set; }
    [Key(3)] public int Mode { get; set; }
    [Key(4)] public List<ClusterMemberInfo> Members { get; set; } = new();
}

[MessagePackObject]
public class ClusterMemberInfo
{
    [Key(0)] public string NodeId { get; set; } = "";
    [Key(1)] public string Endpoint { get; set; } = "";
}

[MessagePackObject]
public class NodeStatus
{
    [Key(0)] public string NodeId { get; set; } = "";
    [Key(1)] public string Role { get; set; } = "";
    [Key(2)] public long Term { get; set; }
    [Key(3)] public string? LeaderId { get; set; }
    [Key(4)] public ulong LatestSeq { get; set; }
    [Key(5)] public ulong CommitSeq { get; set; }
    [Key(6)] public int Mode { get; set; }
    [Key(7)] public List<PeerInfo> Peers { get; set; } = new();
    [Key(8)] public int BootstrapInSyncCount { get; set; }
    [Key(9)] public bool Resyncing { get; set; }
}

[MessagePackObject]
public class PeerInfo
{
    [Key(0)] public string NodeId { get; set; } = "";
    [Key(1)] public ulong MatchSeq { get; set; }
    [Key(2)] public bool Reachable { get; set; }
}

/// <summary>
/// One chunk of one checkpoint file. Files are streamed in contiguous chunks
/// (a new FileName starts a new file) so a single message never exceeds the
/// gRPC max-message size regardless of how large the SST files are.
/// </summary>
[MessagePackObject]
public class ClusterFileData
{
    [Key(0)] public string FileName { get; set; } = string.Empty;
    [Key(1)] public ulong FileSize { get; set; }
    [Key(2)] public byte[] Content { get; set; } = Array.Empty<byte>();
}

[MessagePackObject]
public class SyncUpdatesRequest
{
    [Key(0)] public string NodeId { get; set; } = "";
    [Key(1)] public ulong StartSeq { get; set; }
}

[MessagePackObject]
public class LogConsistencyRequest
{
    [Key(0)] public string NodeId { get; set; } = "";
    [Key(1)] public ulong FollowerLatestSeq { get; set; }
    [Key(2)] public long FollowerTermAtLatest { get; set; }
}

[MessagePackObject]
public class LogConsistencyResponse
{
    [Key(0)] public bool IsLeader { get; set; }
    [Key(1)] public bool Consistent { get; set; }
    [Key(2)] public long Term { get; set; }
}

[MessagePackObject]
public class ReadResponse
{
    [Key(0)] public bool Found { get; set; }
    [Key(1)] public string? Value { get; set; }
}

[MessagePackObject]
public class ChecksumResponse
{
    [Key(0)] public bool Available { get; set; }
    [Key(1)] public long KeyCount { get; set; }
    [Key(2)] public ulong Hash { get; set; }
    [Key(3)] public ulong LatestSeq { get; set; }
}

[MessagePackObject]
[MessagePackFormatter(typeof(PooledClusterBatchDataSerializer))]
public class ClusterBatchData
{
    [Key(0)] public ulong SequenceNumber { get; set; }
    [Key(1)] public int Length { get; set; }
    [Key(2)] public long LeaderTerm { get; set; }
    [Key(3)] public string LeaderId { get; set; } = "";
    /// <summary>
    /// Term of this batch itself (from the leader's term map), as opposed to
    /// <see cref="LeaderTerm"/> which is the leader's current term. A follower
    /// catching up replays old batches whose entry term is lower than the
    /// leader's current term; recording LeaderTerm for them would corrupt the
    /// follower's term map and break the election up-to-date comparison.
    /// </summary>
    [Key(4)] public long EntryTerm { get; set; }
    [Key(5)] public byte[] PooledData { get; set; } = Array.Empty<byte>();

    [IgnoreMember] public ReadOnlySpan<byte> Data => PooledData.AsSpan(0, Length);

    public void ReturnToPool()
    {
        if (PooledData.Length > 0) ArrayPool<byte>.Shared.Return(PooledData);
        PooledData = Array.Empty<byte>();
    }
}

public class PooledClusterBatchDataSerializer : IMessagePackFormatter<ClusterBatchData>
{
    public ClusterBatchData Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        var d = new ClusterBatchData();
        d.SequenceNumber = reader.ReadUInt64();
        d.Length = reader.ReadInt32();
        d.LeaderTerm = reader.ReadInt64();
        d.LeaderId = reader.ReadString() ?? "";
        d.EntryTerm = reader.ReadInt64();
        var pooled = ArrayPool<byte>.Shared.Rent(Math.Max(1, d.Length));
        var raw = reader.ReadRaw(d.Length);
        raw.CopyTo(pooled.AsSpan(0, d.Length));
        d.PooledData = pooled;
        return d;
    }

    public void Serialize(ref MessagePackWriter writer, ClusterBatchData value, MessagePackSerializerOptions options)
    {
        writer.WriteUInt64(value.SequenceNumber);
        writer.WriteInt32(value.Length);
        writer.WriteInt64(value.LeaderTerm);
        writer.Write(value.LeaderId);
        writer.WriteInt64(value.EntryTerm);
        writer.WriteRaw(value.PooledData.AsSpan(0, value.Length));
    }
}

[MessagePackObject]
public class WriteRequest
{
    [Key(0)] public string Key { get; set; } = "";
    [Key(1)] public string Value { get; set; } = "";
}
