#if NET5_0_OR_GREATER
using System;

namespace RocksDbSharp;

public sealed class ClusterMember : IEquatable<ClusterMember>
{
    public string NodeId { get; }
    public string Endpoint { get; }

    public ClusterMember(string nodeId, string endpoint)
    {
        if (string.IsNullOrWhiteSpace(nodeId)) throw new ArgumentException("NodeId is required", nameof(nodeId));
        if (string.IsNullOrWhiteSpace(endpoint)) throw new ArgumentException("Endpoint is required", nameof(endpoint));
        NodeId = nodeId;
        Endpoint = endpoint;
    }

    public bool Equals(ClusterMember? other) => other != null && other.NodeId == NodeId;
    public override bool Equals(object? obj) => obj is ClusterMember m && Equals(m);
    public override int GetHashCode() => NodeId.GetHashCode(StringComparison.Ordinal);
    public override string ToString() => $"{NodeId}@{Endpoint}";
}
#endif
