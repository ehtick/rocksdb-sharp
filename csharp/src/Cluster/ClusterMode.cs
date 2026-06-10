#if NET5_0_OR_GREATER
namespace RocksDbSharp;

/// <summary>
/// Two phases of the clustering database life cycle.
///
/// Bootstrap: one designated primary accepts writes while peers catch up via the
/// existing snapshot + WAL replication path. No election is performed in this
/// phase.
///
/// Cluster: full Raft is in effect. The leader is elected by quorum and any
/// member may be promoted after a leader failure.
/// </summary>
public enum ClusterMode
{
    Bootstrap = 0,
    Cluster = 1,
}
#endif
