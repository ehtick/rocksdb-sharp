using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using MagicOnion.Client;
using RocksDbSharp;

namespace ClusterTest;

/// <summary>
/// Implements the IPeerTransport contract that RaftClusterNode uses to fan out
/// RequestVote / Heartbeat RPCs. Maintains one MagicOnion client per peer.
/// </summary>
internal sealed class ClusterPeerTransport : IPeerTransport, IDisposable
{
    private readonly ConcurrentDictionary<string, (GrpcChannel ch, IClusterService client)> _clients = new();

    private (GrpcChannel ch, IClusterService client) GetClient(ClusterMember peer)
    {
        return _clients.GetOrAdd(peer.NodeId, _ =>
        {
            var ch = GrpcChannel.ForAddress(peer.Endpoint);
            var client = MagicOnionClient.Create<IClusterService>(ch);
            return (ch, client);
        });
    }

    public async Task<RequestVoteResult?> SendRequestVoteAsync(
        ClusterMember peer, long term, string candidateId, ulong lastLogSeq, long lastLogTerm, CancellationToken ct)
    {
        try
        {
            var (_, client) = GetClient(peer);
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(2));
            var resp = await client.RequestVoteAsync(new RequestVoteRequest
            {
                Term = term,
                CandidateId = candidateId,
                LastLogSeq = lastLogSeq,
                LastLogTerm = lastLogTerm,
            });
            return new RequestVoteResult(resp.Term, resp.VoteGranted);
        }
        catch { return null; }
    }

    public async Task<AppendEntriesResult?> SendHeartbeatAsync(ClusterMember peer, HeartbeatPayload payload, CancellationToken ct)
    {
        try
        {
            var (_, client) = GetClient(peer);
            var resp = await client.HeartbeatAsync(new HeartbeatRequest
            {
                Term = payload.Term,
                LeaderId = payload.LeaderId,
                PrevLogSeq = payload.PrevLogSeq,
                PrevLogTerm = payload.PrevLogTerm,
                LeaderCommit = payload.LeaderCommit,
                LeaderMode = (int)payload.LeaderMode,
            });
            return new AppendEntriesResult(resp.Term, resp.Success, resp.LastSeq, resp.Reason);
        }
        catch { return null; }
    }

    public void Dispose()
    {
        foreach (var entry in _clients.Values)
        {
            try { entry.ch.Dispose(); } catch { }
        }
        _clients.Clear();
    }
}
