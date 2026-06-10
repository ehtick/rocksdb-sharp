#if NET5_0_OR_GREATER
using System;
using System.Collections.Generic;
using System.Linq;

namespace RocksDbSharp;

public sealed class RaftConfig
{
    public string NodeId { get; }
    public string Endpoint { get; }
    public IReadOnlyList<ClusterMember> Peers { get; }
    public bool BootstrapPrimary { get; }

    public int ElectionTimeoutMinMs { get; }
    public int ElectionTimeoutMaxMs { get; }
    public int HeartbeatIntervalMs { get; }

    /// <summary>
    /// When in Bootstrap mode, how close to the primary (in WAL sequence numbers)
    /// a follower must be before it is considered "in-sync" and counts towards
    /// the quorum needed to switch to Cluster mode.
    /// </summary>
    public ulong BootstrapInSyncLagThreshold { get; }

    /// <summary>
    /// Minimum number of voting members that must be in-sync (including the
    /// bootstrap primary itself) before the cluster switches to Cluster mode.
    /// Defaults to a strict majority of the configured peers + self.
    /// </summary>
    public int BootstrapPromotionQuorum { get; }

    public RaftConfig(
        string nodeId,
        string endpoint,
        IEnumerable<ClusterMember> peers,
        bool bootstrapPrimary = false,
        int electionTimeoutMinMs = 1500,
        int electionTimeoutMaxMs = 3000,
        int heartbeatIntervalMs = 300,
        ulong bootstrapInSyncLagThreshold = 1000,
        int bootstrapPromotionQuorum = -1)
    {
        if (string.IsNullOrWhiteSpace(nodeId)) throw new ArgumentException("NodeId required", nameof(nodeId));
        if (string.IsNullOrWhiteSpace(endpoint)) throw new ArgumentException("Endpoint required", nameof(endpoint));
        if (electionTimeoutMinMs <= 0) throw new ArgumentOutOfRangeException(nameof(electionTimeoutMinMs));
        if (electionTimeoutMaxMs < electionTimeoutMinMs) throw new ArgumentOutOfRangeException(nameof(electionTimeoutMaxMs));
        if (heartbeatIntervalMs <= 0 || heartbeatIntervalMs >= electionTimeoutMinMs)
            throw new ArgumentOutOfRangeException(nameof(heartbeatIntervalMs), "Heartbeat must be smaller than ElectionTimeoutMin");

        NodeId = nodeId;
        Endpoint = endpoint;
        Peers = peers?.ToList() ?? throw new ArgumentNullException(nameof(peers));
        BootstrapPrimary = bootstrapPrimary;
        ElectionTimeoutMinMs = electionTimeoutMinMs;
        ElectionTimeoutMaxMs = electionTimeoutMaxMs;
        HeartbeatIntervalMs = heartbeatIntervalMs;
        BootstrapInSyncLagThreshold = bootstrapInSyncLagThreshold;

        int total = Peers.Count + 1;
        BootstrapPromotionQuorum = bootstrapPromotionQuorum > 0
            ? bootstrapPromotionQuorum
            : (total / 2) + 1;
    }

    /// <summary>
    /// Quorum threshold (majority) including self.
    /// </summary>
    public int Quorum => ((Peers.Count + 1) / 2) + 1;
}
#endif
