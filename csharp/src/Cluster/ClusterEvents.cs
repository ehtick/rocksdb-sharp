#if NET5_0_OR_GREATER
using System;

namespace RocksDbSharp;

public sealed class LeaderChangedEventArgs : EventArgs
{
    public string? LeaderId { get; }
    public long Term { get; }
    public LeaderChangedEventArgs(string? leaderId, long term) { LeaderId = leaderId; Term = term; }
}

public sealed class ModeChangedEventArgs : EventArgs
{
    public ClusterMode Mode { get; }
    public ModeChangedEventArgs(ClusterMode mode) { Mode = mode; }
}

public sealed class RoleChangedEventArgs : EventArgs
{
    public RaftRole Role { get; }
    public long Term { get; }
    public RoleChangedEventArgs(RaftRole role, long term) { Role = role; Term = term; }
}
#endif
