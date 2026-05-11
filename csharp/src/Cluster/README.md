# RocksDbSharp Clustering

This folder contains a transport-agnostic Raft implementation that turns the
existing single-primary/single-replica WAL streaming into a fault-tolerant,
multi-node clustering database. The Raft layer reuses the snapshot + WAL
replication primitives unchanged - this code only adds:

- term-and-vote bookkeeping,
- leader election,
- heartbeat-driven liveness detection,
- quorum-based commit advancement,
- a "bootstrap" initialization phase that lets one primary keep serving writes
  until the cluster has caught up enough to switch on full Raft.

The MagicOnion service that wires these classes onto gRPC lives in
`Tests/ClusterTest/`. Nothing in this folder depends on gRPC, MagicOnion, or
ASP.NET, so the same library can be embedded in any transport.

---

## File map

| File | Purpose |
|------|---------|
| `RaftRole.cs` | `Follower / Candidate / Leader` enum. |
| `ClusterMode.cs` | `Bootstrap / Cluster` enum (life-cycle phase). |
| `ClusterMember.cs` | `(NodeId, Endpoint)` value object for cluster peers. |
| `RaftConfig.cs` | Per-node configuration: peers, election + heartbeat timings, bootstrap quorum and in-sync threshold. |
| `RaftPersistentState.cs` | Crash-safe `(currentTerm, votedFor, mode)` plus the term-range log; written via atomic temp-file rename. |
| `RaftLogTerms.cs` | Sparse `(startSeq -> term)` map. Binary-searchable. Lets a node answer "what was the term of WAL entry N?". |
| `PeerState.cs` | Per-follower `matchSeq / nextSeq / reachable` tracking maintained by the leader. |
| `ElectionTimer.cs` | Randomized election timeout in `[MinMs, MaxMs]`, reset by heartbeats. |
| `RaftClusterNode.cs` | The orchestrator. Owns role transitions, election state, leader heartbeat fan-out and commit advancement. Talks to peers via `IPeerTransport`. |
| `ClusterEvents.cs` | `LeaderChanged / RoleChanged / ModeChanged` event args. |

---

## Why "RocksDB seqNo is the Raft log index"

Classical Raft has its own append-only log. We do not - the RocksDB WAL is
already an append-only log of `(seqNo, batchBytes)` entries with all the
durability guarantees we need. So:

- **Raft log index** = RocksDB sequence number (`db.GetLatestSequenceNumber()`).
- **Log replication** = the existing `ReplicationSource.GetPooledWalUpdates(seq)`
  streamed over gRPC.
- **Catch-up from far behind** = the existing `ReplicationSource.GetInitialState`
  checkpoint transfer (`SyncInitialStateAsync` on the wire).

What Raft additionally needs is a **term** attached to every log entry. Storing
a term inside every WriteBatch would require modifying RocksDB. Instead we
keep a tiny **side-log of term ranges** in `raft.terms`: each row is
`(startSeq, term)` and any entry's term is the term of the largest
`startSeq <= seqNo`. The leader appends a new row on every leadership win, and
followers append a new row whenever they observe a batch tagged with an unseen
term in the WAL stream.

That side-log plus `(currentTerm, votedFor, mode)` is the only Raft state
written to disk. Everything else is derivable.

---

## Life cycle

### 1. Bootstrap (initialization)

The cluster starts with **one** designated primary
(`RaftConfig.BootstrapPrimary = true`). On startup:

- The primary opens its RocksDB normally and is **forced into the Leader role
  without holding an election**. It writes `currentTerm = 1, votedFor = self`
  to disk and records the term-range row `(latestSeq + 1 -> 1)`.
- Every other node, on first boot, finds an empty data directory, looks up the
  bootstrap primary from its config, and pulls a full checkpoint via
  `SyncInitialStateAsync`. Then it opens its local DB and starts the
  follower-stream (`SyncUpdatesAsync`) from `latestSeq + 1`.
- Followers receive WAL batches tagged with the leader's current term and
  leader id. As they ingest they:
  - call `Node.HandleObservedLeader(leaderId, term)` to update their view of
    who the leader is and what term they're in,
  - append a new term-range row whenever the batch's term differs from the
    last one they recorded.
- Periodically the follower stream calls
  `ReportLastSyncSequenceNumber(nodeId, latestSeq)` so the leader can update
  the follower's `matchSeq`.
- While `Mode == Bootstrap`, **no election is allowed**. `HandleRequestVote`
  refuses every vote and the follower's election timer resets itself instead
  of starting a candidate run. This prevents split-brain before the cluster
  has any replicated data to vote on.

The primary keeps accepting writes the entire time. Bootstrap is what lets
the cluster be useful before it has converged.

### 2. Promotion to Cluster mode

After each successful `ReportLastSyncSequenceNumber` the leader runs
`MaybePromoteToCluster()`:

- Count followers whose `matchSeq + lagThreshold >= latestSeq` (i.e. within
  `BootstrapInSyncLagThreshold` WAL records of the leader). Add 1 for self.
- If that count reaches `BootstrapPromotionQuorum`
  (defaults to a strict majority of `peers + 1`), the leader writes
  `mode = Cluster` to its own `raft.state` and raises `ModeChanged`.
- The new mode is broadcast by piggybacking on the next heartbeat
  (`HeartbeatPayload.LeaderMode`). Each follower's `HandleAppendEntries`
  promotes itself to Cluster mode when it sees a heartbeat with
  `LeaderMode == Cluster`, persists that flag and raises its own
  `ModeChanged`.

Once a node has flipped to Cluster mode, it never goes back. The transition
is one-way.

### 3. Steady-state Cluster mode

In Cluster mode every node enforces full Raft:

- **Leader** sends a heartbeat to each peer every `HeartbeatIntervalMs` ms
  (default 250-300ms). The heartbeat carries
  `(term, leaderId, prevLogSeq, prevLogTerm, leaderCommit, leaderMode)` -
  no batch data; data flows on the long-lived `SyncUpdatesAsync` stream.
- **Follower** resets its election timer on every accepted heartbeat. If the
  randomized timer (default `[1500, 3000]` ms) elapses without a heartbeat, it
  becomes a candidate.
- **Commit advancement**: the leader keeps a `matchSeq` per peer; on each
  reported progress it sorts the values plus its own `latestSeq` descending,
  picks the value at quorum index (`majority-1`), and that becomes the new
  `commitSeq`. Followers receive `leaderCommit` on the next heartbeat and
  advance theirs accordingly.

---

## Election protocol

The election logic lives in `RaftClusterNode.OnElectionTimeout` /
`RunElectionAsync`. It follows §5.2 of the Raft paper with the obvious
mapping from "log index" to "WAL seqNo".

1. Election timer fires on a Follower (or a Candidate whose previous election
   was inconclusive).
2. The node transitions to `Candidate`, bumps `currentTerm`, votes for self,
   and resets the election timer with a new random value.
3. It sends `RequestVote(term, candidateId, lastLogSeq, lastLogTerm)` to all
   peers in parallel.
4. Each peer's `HandleRequestVote` grants the vote iff **all** of these hold:
   - the request's `term >= currentTerm` (the receiver may step down and bump
     its term first);
   - `Mode == Cluster` (we never vote during Bootstrap);
   - `votedFor` is null or already equal to `candidateId` for this term;
   - the candidate's `(lastLogTerm, lastLogSeq)` is at least as up-to-date as
     ours (lexicographic compare): a candidate with a stale log cannot win.
5. The candidate counts grants. As soon as it has `Quorum` votes
   (including its own), it calls `BecomeLeader()`:
   - record a new term-range row at `latestSeq + 1`,
   - reset every peer's `nextSeq` to `latestSeq + 1`,
   - pause the election timer,
   - start the heartbeat loop, which immediately notifies all peers of the new
     term and leader id.
6. If the candidate sees any response with a higher term, it steps down to
   Follower for that term.
7. If the election timer fires again before a quorum, the candidate starts a
   **new** election in the next term. The randomized timeout makes a split
   vote unlikely to repeat.

Persistence guarantees during elections:

- `currentTerm` is fsync'd (atomic temp-file rename) **before** any
  `RequestVote` is sent or any vote is granted.
- `votedFor` is fsync'd before the granting reply is returned.
- The new term-range row is fsync'd inside `BecomeLeader` before any
  heartbeat with the new term goes out.

This is what makes the algorithm safe across crashes - a candidate that wins
then dies cannot come back as a follower of a lower term and accept
conflicting log entries.

---

## Failure modes

### Leader process crash (kill -9)

The leader simply disappears. From every follower's perspective heartbeats
stop arriving and `SyncUpdatesAsync` returns an end-of-stream / Unavailable.
The follower stream task logs the error and starts retrying every second; in
parallel the election timer keeps running and fires within
`[ElectionTimeoutMinMs, ElectionTimeoutMaxMs]` of the last heartbeat. The
first follower to time out wins the election (provided its log is at least as
up-to-date as a quorum's), promotes itself, and starts sending heartbeats.

Empirically the path takes ~2-3 seconds: ~election-timeout to detect, plus
the round-trip of the first heartbeat-cycle from the new leader.

When the dead leader is restarted, its data directory is intact. It reads
back `(currentTerm = T, votedFor, mode = Cluster, terms log)` from disk. The
first heartbeat it receives from the new leader carries `term > T`, so
`HandleAppendEntries` bumps its term, calls `BecomeFollower(newLeader)`, and
the WAL stream picks up where it left off.

### Leader hang (SIGSTOP / GC pause / blocked thread)

This is the hardest failure to distinguish from a partition - the process
is alive but not responding. Detection is purely timeout-based:

- The hung leader stops sending heartbeats.
- Followers time out and run an election. The hung leader is unreachable so
  it does not get to vote, but its absence doesn't matter as long as a
  quorum of the other nodes is reachable.
- A new leader is elected at a higher term.

When the hung process resumes (SIGCONT, GC finishes, etc.):

- Its own heartbeat fan-out will fail or return responses carrying the higher
  term. Either condition triggers `BecomeFollower(null)` and persists the new
  term.
- The next heartbeat from the new leader confirms it and re-attaches the
  follower stream.

There is a small write-visibility window: between the moment the leader
freezes and the moment it discovers the higher term, it would happily accept
client writes (it still believes it is leader). Those writes never reach a
quorum so they never commit; when the node steps down, RocksDB's WAL still
contains them. They will be **overwritten on resync if the new leader's log
diverges**: the follower's `HandleAppendEntries` returns
`Success = false, Reason = "term-mismatch"` and the host code re-snapshots
from the leader via `SyncInitialStateAsync`. From a client's point of view
the uncommitted writes simply never happened - they were never acknowledged.

This is the standard Raft trade-off: only **committed** writes are durable.
Uncommitted writes on a deposed leader are silently dropped.

### Follower process crash

The leader's heartbeat to that peer fails; `peer.OnAppendFailed()` flips
`reachable = false`. Commit advancement is not affected as long as a quorum
of the remaining followers acknowledge.

On restart the follower:

- reads back its persistent Raft state and its RocksDB WAL,
- starts as Follower (it is not the bootstrap primary on restart - the
  bootstrap path is gated on an empty data directory),
- the host code reconnects `SyncUpdatesAsync(startSeq = latestSeq + 1)` to the
  current leader,
- receives heartbeats and resumes log replication from where it left off.

If the follower has been down long enough that the leader has truncated WAL
files past its `latestSeq`, the next batch will produce a sequence-number gap
when the consumer tries to ingest, and the host falls back to a full
`SyncInitialStateAsync` snapshot (this is exactly the same recovery path
that the existing single-replica setup uses).

### Network partition

A partition is the same as "leader hang" from each side's perspective: each
side sees timeouts on the other side.

- The side **containing a majority** continues without disruption. If it
  already contains the leader, nothing changes - heartbeats keep flowing
  inside the majority partition. If it doesn't, the followers time out,
  elect a new leader at a higher term, and keep going.
- The side **without a majority** cannot elect anyone, because `Quorum`
  requires `(N/2)+1` votes. The minority's old-leader (if any) keeps
  accepting writes locally but those writes will never reach commit -
  `TryAdvanceCommit` sorts `matchSeq` values and picks the quorum-th entry,
  and on the minority side there isn't one.
- When the partition heals, the side that fell behind discovers the higher
  term via heartbeats, steps down, and re-syncs - exactly like the "leader
  hang resumes" scenario above. Any non-committed writes that the minority
  leader accepted are dropped during the term-mismatch resync.

This is the safety guarantee that makes Raft non-split-brain: at most one
side of a partition can ever achieve quorum, so at most one side can produce
new committed writes.

### Simultaneous failures

The cluster tolerates `floor((N-1)/2)` simultaneous failures. With the
default 3-node setup, that's one failure (any node, any reason). Lose two
out of three and the survivor cannot make progress until at least one peer
comes back.

### Disk corruption / state file truncation

`RaftPersistentState.Persist` writes to `raft.state.tmp` then `File.Move`'s
it over `raft.state` (and analogously for `raft.terms`). A crash mid-write
leaves the old file intact, so on recovery we always read a coherent
`(term, vote, mode)` tuple. If both files are physically corrupt (e.g.
hardware failure), `Load` falls back to `(term = 0, vote = null, mode = 0)`;
the node will then re-snapshot from the current leader.

---

## What this implementation does NOT do

To keep the scope tight, a few standard Raft extensions are deliberately out
of scope:

- **Pre-vote** (§9.6). A partitioned follower that rejoins will bump its term
  and trigger one election round before stabilizing. Pre-vote would avoid
  that small disturbance but isn't required for safety.
- **Dynamic membership changes** (§6 of the dissertation). The peer list is
  static for a process lifetime. Adding/removing nodes requires a config
  update and a restart.
- **Log compaction inside Raft** (§7). We piggyback on RocksDB's own
  compaction - the existing replication code already prunes WAL files once
  every follower has acknowledged past them, via
  `RocksDbWalInspector.GetFirstSequenceNumbers`.
- **Linearizable reads from followers**. Reads currently only have leader
  linearizability (clients should query the leader). Adding ReadIndex would
  let followers serve linearizable reads without a round-trip.
- **Batched / pipelined AppendEntries**. Heartbeats are sent one-per-peer
  every `HeartbeatIntervalMs`; the data plane is the long-lived
  `SyncUpdatesAsync` stream, which already does pipelining via gRPC.

---

## Configuration cheatsheet

```csharp
new RaftConfig(
    nodeId: "node1",
    endpoint: "http://localhost:51000",
    peers: new[]
    {
        new ClusterMember("node2", "http://localhost:51001"),
        new ClusterMember("node3", "http://localhost:51002"),
    },
    bootstrapPrimary: true,           // exactly one node sets this to true
    electionTimeoutMinMs: 1500,       // randomized in [Min, Max]
    electionTimeoutMaxMs: 3000,
    heartbeatIntervalMs: 250,         // must be << ElectionTimeoutMinMs
    bootstrapInSyncLagThreshold: 50_000,
    bootstrapPromotionQuorum: 2);     // default: majority of peers + self
```

Tuning notes:

- `HeartbeatIntervalMs` should be at most `ElectionTimeoutMinMs / 3` to leave
  headroom for transient delays.
- `BootstrapInSyncLagThreshold` is in WAL records, not bytes. With ~10k
  records/sec write rate, `50_000` is ~5 seconds behind.
- `Quorum` is always `(peers+1)/2 + 1` and is computed from the peer list.

---

## End-to-end flow

```
+--------+          snapshot           +----------+
| primary| ----------------------------> follower |
| (boot) |                              | (init)   |
+--------+                              +----------+
     |                                       |
     |  SyncUpdatesAsync (WAL stream)        |
     |-------------------------------------->|
     |                                       |
     |    ReportLastSyncSequenceNumber       |
     |<--------------------------------------|
     |  (matchSeq advances, bootSync count   |
     |   updates, promotion check fires)     |
     |                                       |
     +--- ModeChanged: Cluster --------------+
     |                                       |
     |   periodic HeartbeatAsync ------>     |
     |   (term, leaderCommit, leaderMode)    |
     |                                       |
     +-- on leader failure -----------------+
                                              |
                                       election timer fires
                                              |
                                       RequestVoteAsync to peers
                                              |
                                       quorum reached -> BecomeLeader
                                              |
                                       new heartbeat: term++
```

See `Tests/ClusterTest/Program.cs` for a working coordinator that drives the
four canonical scenarios end-to-end: `election`, `crash`, `hang`, `rejoin`.
