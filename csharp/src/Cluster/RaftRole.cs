#if NET5_0_OR_GREATER
namespace RocksDbSharp;

public enum RaftRole
{
    Follower = 0,
    Candidate = 1,
    Leader = 2,
}
#endif
