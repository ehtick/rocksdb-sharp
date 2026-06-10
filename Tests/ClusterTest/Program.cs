using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using MagicOnion.Client;

namespace ClusterTest;

internal static class Program
{
    static async Task<int> Main(string[] args)
    {
        if (args.Length == 0 || args[0] == "coordinator")
        {
            return await new Coordinator().RunAsync(args.Skip(1).ToArray());
        }

        if (args[0] == "node")
        {
            string cfgPath = args[1];
            var cfg = JsonSerializer.Deserialize<NodeConfiguration>(await File.ReadAllTextAsync(cfgPath))
                      ?? throw new Exception("Bad config file");

            var host = new ClusterNodeHost(cfg);
            await host.StartAsync();

            // The "writer" role drives traffic when this node is the leader.
            // Every Put goes through TryWriteAsLeader so a deposed leader stops
            // writing immediately instead of polluting its local WAL.
            _ = Task.Run(async () =>
            {
                long counter = 0;
                while (true)
                {
                    if (host.WriteLoadEnabled)
                    {
                        for (int i = 0; i < 200; i++)
                        {
                            string key = $"{host.Node.NodeId}_{counter:D012}";
                            if (!host.TryWriteAsLeader(key, $"{Stopwatch.GetTimestamp()}")) break;
                            counter++;
                        }
                    }
                    await Task.Delay(50);
                }
            });

            // Console reporting once per two seconds
            _ = Task.Run(async () =>
            {
                while (true)
                {
                    try
                    {
                        ulong latest = host.IsResyncing ? 0 : host.Db.GetLatestSequenceNumber();
                        Console.WriteLine($"[{DateTimeOffset.UtcNow:HH:mm:ss.fff} {host.Node.NodeId}] " +
                                          $"role={host.Node.Role} term={host.Node.CurrentTerm} " +
                                          $"leader={host.Node.CurrentLeaderId} " +
                                          $"latest={latest:n0} " +
                                          $"commit={host.Node.CommitSeq:n0} mode={host.Node.Mode} " +
                                          $"bootSync={host.Node.BootstrapInSyncCount}" +
                                          (host.IsResyncing ? " RESYNCING" : ""));
                    }
                    catch { }
                    await Task.Delay(2000);
                }
            });

            // Keep the process alive until killed.
            await Task.Delay(-1);
            return 0;
        }

        Console.Error.WriteLine($"Unknown mode: {args[0]}");
        return 1;
    }
}

internal sealed class Coordinator
{
    private readonly string _tempRoot;
    private readonly string _exePath;
    private readonly string _argsPrefix;
    private readonly List<NodeProcess> _processes = new();

    // ---- Raft invariant tracking (fed by every status poll) ----
    private readonly object _invariantGate = new();
    private readonly Dictionary<string, long> _highestTermSeen = new();   // nodeId -> term
    private readonly Dictionary<long, string> _leaderByTerm = new();      // term -> leaderId
    private readonly List<string> _violations = new();

    public Coordinator()
    {
        _tempRoot = Path.Combine(Path.GetTempPath(), "RocksDbClusterTest_" + DateTime.UtcNow.Ticks);
        Directory.CreateDirectory(_tempRoot);

        _exePath = Process.GetCurrentProcess().MainModule?.FileName ?? "dotnet";
        // If we are hosted by `dotnet ...ClusterTest.dll`, re-launch children with
        // the same DLL so they reuse the compiled output rather than running
        // `dotnet run` which would re-build for every spawn.
        if (_exePath.EndsWith("dotnet") || _exePath.EndsWith("dotnet.exe"))
        {
            string? dllPath = typeof(Coordinator).Assembly.Location;
            if (!string.IsNullOrEmpty(dllPath) && File.Exists(dllPath))
            {
                _argsPrefix = $"\"{dllPath}\" ";
            }
            else
            {
                _argsPrefix = "run --project Tests/ClusterTest/ClusterTest.csproj -- ";
            }
        }
        else
        {
            _argsPrefix = "";
        }
    }

    public async Task<int> RunAsync(string[] args)
    {
        string scenario = args.Length > 0 ? args[0] : "all";

        Log($"temp dir: {_tempRoot}");
        try
        {
            switch (scenario)
            {
                case "election": await ScenarioElectionAsync(); break;
                case "crash":    await ScenarioLeaderCrashAsync(); break;
                case "hang":     await ScenarioLeaderHangAsync(); break;
                case "rejoin":   await ScenarioFollowerCrashRejoinAsync(); break;
                case "all":
                    await ScenarioElectionAsync();
                    await ScenarioLeaderCrashAsync();
                    await ScenarioLeaderHangAsync();
                    await ScenarioFollowerCrashRejoinAsync();
                    break;
                default:
                    Console.Error.WriteLine($"Unknown scenario '{scenario}'. Use: election | crash | hang | rejoin | all");
                    return 2;
            }
            Log("ALL SCENARIOS PASSED");
            return 0;
        }
        catch (Exception ex)
        {
            Log($"FAILED: {ex.Message}");
            return 1;
        }
        finally
        {
            KillAll();
        }
    }

    // -----------------------------------------------------------------------
    // Scenarios
    // -----------------------------------------------------------------------

    private async Task ScenarioElectionAsync()
    {
        Log("=== scenario: bootstrap → cluster mode promotion ===");
        var cluster = StartCluster(3, scenarioName: "election", basePort: 51000);

        await WaitForClusterModeAsync(cluster, TimeSpan.FromSeconds(45));
        Log("cluster has switched to Cluster mode");

        var leader = await WaitForLeaderAsync(cluster, TimeSpan.FromSeconds(20));
        Log($"current leader = {leader}");

        // Committed (quorum-acknowledged) writes must succeed on the leader.
        var keys = await WriteCommittedKeysAsync(EndpointOf(cluster, leader), "committed_election", 50);
        Log($"wrote {keys.Count} committed keys");

        await ValidateClusterConsistencyAsync(cluster);

        KillAll();
        await Task.Delay(500);
    }

    private async Task ScenarioLeaderCrashAsync()
    {
        Log("=== scenario: leader crash → re-election ===");
        var cluster = StartCluster(3, scenarioName: "crash", basePort: 51010);
        await WaitForClusterModeAsync(cluster, TimeSpan.FromSeconds(45));
        var leader = await WaitForLeaderAsync(cluster, TimeSpan.FromSeconds(20));
        Log($"initial leader = {leader}");

        // These writes are acknowledged as committed, so Raft's Leader
        // Completeness property says they must survive the crash.
        var keys = await WriteCommittedKeysAsync(EndpointOf(cluster, leader), "committed_crash", 100);
        Log($"wrote {keys.Count} committed keys on {leader}");

        var leaderProc = cluster.First(c => c.Config.NodeId == leader);
        Log($"killing leader {leader} (pid {leaderProc.Process.Id})");
        leaderProc.Process.Kill(entireProcessTree: true);
        await Task.Delay(500);

        // remaining nodes should elect a new leader
        var remaining = cluster.Where(c => c.Config.NodeId != leader).ToList();
        var newLeader = await WaitForLeaderAsync(remaining, TimeSpan.FromSeconds(30), differentFrom: leader);
        Log($"new leader = {newLeader}");

        await VerifyKeysPresentAsync(EndpointOf(remaining, newLeader), keys,
            $"all committed writes survive the failover to {newLeader}");
        Log("all committed writes survived the failover");

        await ValidateClusterConsistencyAsync(remaining);

        KillAll();
        await Task.Delay(500);
    }

    private async Task ScenarioLeaderHangAsync()
    {
        Log("=== scenario: leader hang (SIGSTOP) → re-election → SIGCONT ===");
        var cluster = StartCluster(3, scenarioName: "hang", basePort: 51020);
        await WaitForClusterModeAsync(cluster, TimeSpan.FromSeconds(45));
        var leader = await WaitForLeaderAsync(cluster, TimeSpan.FromSeconds(20));
        Log($"initial leader = {leader}");

        var keysBefore = await WriteCommittedKeysAsync(EndpointOf(cluster, leader), "committed_prehang", 50);

        var leaderProc = cluster.First(c => c.Config.NodeId == leader);
        Log($"freezing leader {leader} (pid {leaderProc.Process.Id})");
        if (!StopProcess(leaderProc.Process))
        {
            Log("SIGSTOP unavailable on this OS; skipping the hang scenario");
            KillAll();
            return;
        }

        // followers should elect a new leader once the heartbeat lapses
        var remaining = cluster.Where(c => c.Config.NodeId != leader).ToList();
        var newLeader = await WaitForLeaderAsync(remaining, TimeSpan.FromSeconds(30), differentFrom: leader);
        Log($"new leader (while old is frozen) = {newLeader}");

        var keysAfter = await WriteCommittedKeysAsync(EndpointOf(remaining, newLeader), "committed_posthang", 50);
        Log($"wrote {keysAfter.Count} committed keys on the new leader");

        // resume the frozen node and confirm it becomes a follower of the new leader
        Log($"resuming {leader}");
        ContinueProcess(leaderProc.Process);

        await WaitUntilAsync(async () =>
        {
            var s = await TryGetStatusAsync(leaderProc.Config.Endpoint);
            return s != null && s.LeaderId == newLeader && s.Role == "Follower";
        }, TimeSpan.FromSeconds(20), $"resumed node {leader} accepts {newLeader} as leader");

        await VerifyKeysPresentAsync(EndpointOf(cluster, newLeader), keysBefore.Concat(keysAfter).ToList(),
            "committed writes from before and after the hang are all present");

        // The resumed node may have accepted uncommitted writes while it still
        // believed it was the leader; it must detect the divergence, resync,
        // and converge to a byte-identical copy. The consistency check below
        // proves both the resync path and the loss of unacknowledged writes.
        await ValidateClusterConsistencyAsync(cluster, timeout: TimeSpan.FromSeconds(90));

        KillAll();
        await Task.Delay(500);
    }

    private async Task ScenarioFollowerCrashRejoinAsync()
    {
        Log("=== scenario: follower crash → restart → catch-up ===");
        var cluster = StartCluster(3, scenarioName: "rejoin", basePort: 51030);
        await WaitForClusterModeAsync(cluster, TimeSpan.FromSeconds(45));
        var leader = await WaitForLeaderAsync(cluster, TimeSpan.FromSeconds(20));

        var follower = cluster.First(c => c.Config.NodeId != leader && !c.Config.BootstrapPrimary);
        Log($"killing follower {follower.Config.NodeId} (pid {follower.Process.Id})");
        follower.Process.Kill(entireProcessTree: true);
        await Task.Delay(1500);

        Log($"restarting follower {follower.Config.NodeId}");
        var restarted = StartNodeProcess(follower.Config);
        cluster.Add(restarted);

        await WaitUntilAsync(async () =>
        {
            var s = await TryGetStatusAsync(follower.Config.Endpoint);
            if (s == null || s.Resyncing) return false;
            var leaderStatus = await TryGetStatusAsync(EndpointOf(cluster, leader));
            if (leaderStatus == null) return false;
            return s.LatestSeq + 5000 >= leaderStatus.LatestSeq;
        }, TimeSpan.FromSeconds(60), $"follower {follower.Config.NodeId} caught back up");

        var alive = cluster.Where(c => !c.Process.HasExited).ToList();
        await ValidateClusterConsistencyAsync(alive);

        KillAll();
        await Task.Delay(500);
    }

    // -----------------------------------------------------------------------
    // Raft validation helpers
    // -----------------------------------------------------------------------

    /// <summary>
    /// Fed with every status poll. Checks the observable Raft invariants:
    ///  - Election Safety: at most one leader per term;
    ///  - terms are monotonically non-decreasing on every node;
    ///  - the commit index never exceeds the log tail.
    /// Violations are recorded and surfaced at the next validation point
    /// (throwing here would be swallowed by the polling loops).
    /// </summary>
    private void CheckInvariants(NodeStatus s)
    {
        lock (_invariantGate)
        {
            if (_highestTermSeen.TryGetValue(s.NodeId, out var prev) && s.Term < prev)
                _violations.Add($"term went backwards on {s.NodeId}: {prev} -> {s.Term}");
            _highestTermSeen[s.NodeId] = Math.Max(s.Term, _highestTermSeen.GetValueOrDefault(s.NodeId));

            if (s.Role == "Leader")
            {
                if (_leaderByTerm.TryGetValue(s.Term, out var other) && other != s.NodeId)
                    _violations.Add($"ELECTION SAFETY VIOLATION: two leaders in term {s.Term}: {other} and {s.NodeId}");
                _leaderByTerm[s.Term] = s.NodeId;
            }

            if (!s.Resyncing && s.CommitSeq > s.LatestSeq)
                _violations.Add($"commit ({s.CommitSeq}) ahead of log tail ({s.LatestSeq}) on {s.NodeId}");
        }
    }

    private void AssertNoViolations()
    {
        lock (_invariantGate)
        {
            if (_violations.Count > 0)
                throw new InvalidOperationException("Raft invariant violations:\n  " + string.Join("\n  ", _violations.Distinct()));
        }
    }

    private async Task<List<string>> WriteCommittedKeysAsync(string leaderEndpoint, string prefix, int count)
    {
        var keys = new List<string>(count);
        using var ch = GrpcChannel.ForAddress(leaderEndpoint);
        var client = MagicOnionClient.Create<IClusterService>(ch);
        for (int i = 0; i < count; i++)
        {
            string key = $"{prefix}_{i:D6}";
            bool ok = await client.WriteAsync(new WriteRequest { Key = key, Value = $"v{i}" });
            if (!ok) throw new InvalidOperationException($"committed write '{key}' was not acknowledged by {leaderEndpoint}");
            keys.Add(key);
        }
        return keys;
    }

    private async Task VerifyKeysPresentAsync(string endpoint, List<string> keys, string label)
    {
        using var ch = GrpcChannel.ForAddress(endpoint);
        var client = MagicOnionClient.Create<IClusterService>(ch);
        var missing = new List<string>();
        foreach (var key in keys)
        {
            var r = await client.ReadAsync(key);
            if (!r.Found) missing.Add(key);
        }
        if (missing.Count > 0)
            throw new InvalidOperationException(
                $"DURABILITY VIOLATION ({label}): {missing.Count}/{keys.Count} committed keys missing, e.g. {missing[0]}");
    }

    /// <summary>
    /// Quiesces the write load, waits for every node to converge to the same
    /// log tail with a fully advanced commit index, then compares whole-DB
    /// checksums. Identical checksums on all replicas is the State Machine
    /// Safety property made observable.
    /// </summary>
    private async Task ValidateClusterConsistencyAsync(IEnumerable<NodeProcess> nodes, TimeSpan? timeout = null)
    {
        var list = nodes.Where(n => !n.Process.HasExited).ToList();
        Log($"validating consistency across {list.Count} nodes...");

        foreach (var n in list)
        {
            try
            {
                using var ch = GrpcChannel.ForAddress(n.Config.Endpoint);
                var client = MagicOnionClient.Create<IClusterService>(ch);
                await client.SetWriteLoadAsync(false);
            }
            catch { }
        }

        await WaitUntilAsync(async () =>
        {
            var statuses = new List<NodeStatus>();
            foreach (var n in list)
            {
                var s = await TryGetStatusAsync(n.Config.Endpoint);
                if (s == null || s.Resyncing) return false;
                statuses.Add(s);
            }
            if (statuses.Count(s => s.Role == "Leader") != 1) return false;
            if (statuses.Select(s => s.LatestSeq).Distinct().Count() != 1) return false;
            // commit must catch up to the tail once the cluster is quiescent
            return statuses.All(s => s.CommitSeq == s.LatestSeq);
        }, timeout ?? TimeSpan.FromSeconds(45), "all nodes converged to the same committed log tail");

        var sums = new List<(string NodeId, ChecksumResponse Sum)>();
        foreach (var n in list)
        {
            using var ch = GrpcChannel.ForAddress(n.Config.Endpoint);
            var client = MagicOnionClient.Create<IClusterService>(ch);
            var sum = await client.ChecksumAsync();
            if (!sum.Available) throw new InvalidOperationException($"checksum unavailable on {n.Config.NodeId}");
            sums.Add((n.Config.NodeId, sum));
        }

        var first = sums[0];
        foreach (var (nodeId, sum) in sums.Skip(1))
        {
            if (sum.Hash != first.Sum.Hash || sum.KeyCount != first.Sum.KeyCount)
                throw new InvalidOperationException(
                    "STATE MACHINE SAFETY VIOLATION: replicas diverge: " +
                    string.Join(", ", sums.Select(x => $"{x.NodeId}: {x.Sum.KeyCount} keys / {x.Sum.Hash:x16}")));
        }

        AssertNoViolations();
        Log($"consistency OK: {first.Sum.KeyCount:n0} keys, checksum {first.Sum.Hash:x16}, on {sums.Count} nodes");
    }

    private static string EndpointOf(IEnumerable<NodeProcess> cluster, string nodeId) =>
        cluster.First(c => c.Config.NodeId == nodeId).Config.Endpoint;

    // -----------------------------------------------------------------------
    // Cluster bootstrap helpers
    // -----------------------------------------------------------------------

    private List<NodeProcess> StartCluster(int nodeCount, string scenarioName, int basePort)
    {
        // Raft invariants hold within one cluster; each scenario boots a fresh
        // cluster whose terms restart at 1, so the trackers must restart too.
        lock (_invariantGate)
        {
            _highestTermSeen.Clear();
            _leaderByTerm.Clear();
            _violations.Clear();
        }

        var members = new List<MemberConfig>();
        for (int i = 0; i < nodeCount; i++)
        {
            members.Add(new MemberConfig
            {
                NodeId = $"node{i + 1}",
                Endpoint = $"http://localhost:{basePort + i}",
                IsBootstrapPrimary = i == 0,
            });
        }

        var result = new List<NodeProcess>();
        for (int i = 0; i < nodeCount; i++)
        {
            var cfg = new NodeConfiguration
            {
                NodeId = members[i].NodeId,
                Endpoint = members[i].Endpoint,
                Port = basePort + i,
                DbPath = Path.Combine(_tempRoot, scenarioName, $"node{i + 1}"),
                BootstrapPrimary = members[i].IsBootstrapPrimary,
                AllMembers = members,
                ElectionTimeoutMinMs = 1500,
                ElectionTimeoutMaxMs = 3000,
                HeartbeatIntervalMs = 250,
                BootstrapInSyncLagThreshold = 50_000,
            };
            // give the primary a head start so it can serve initial snapshots
            if (i > 0) Thread.Sleep(800);
            result.Add(StartNodeProcess(cfg));
        }
        return result;
    }

    private NodeProcess StartNodeProcess(NodeConfiguration cfg)
    {
        Directory.CreateDirectory(cfg.DbPath);
        string cfgPath = Path.Combine(cfg.DbPath, "node.json");
        File.WriteAllText(cfgPath, JsonSerializer.Serialize(cfg));

        var psi = new ProcessStartInfo
        {
            FileName = _exePath,
            Arguments = $"{_argsPrefix}node \"{cfgPath}\"",
            UseShellExecute = false,
            CreateNoWindow = false,
        };
        var proc = Process.Start(psi)!;
        var np = new NodeProcess(cfg, proc);
        _processes.Add(np);
        Log($"started {cfg.NodeId} pid={proc.Id} on {cfg.Endpoint} (bootstrap={cfg.BootstrapPrimary})");
        return np;
    }

    private void KillAll()
    {
        foreach (var p in _processes)
        {
            try { if (!p.Process.HasExited) p.Process.Kill(entireProcessTree: true); } catch { }
        }
        _processes.Clear();
    }

    // -----------------------------------------------------------------------
    // Wait helpers
    // -----------------------------------------------------------------------

    private async Task WaitForClusterModeAsync(IEnumerable<NodeProcess> cluster, TimeSpan timeout)
    {
        await WaitUntilAsync(async () =>
        {
            int count = 0;
            int inMode = 0;
            foreach (var c in cluster)
            {
                var s = await TryGetStatusAsync(c.Config.Endpoint);
                if (s == null) continue;
                count++;
                if (s.Mode == (int)RocksDbSharp.ClusterMode.Cluster) inMode++;
            }
            return count > 0 && inMode == count;
        }, timeout, "all nodes are in Cluster mode");
    }

    private async Task<string> WaitForLeaderAsync(IEnumerable<NodeProcess> cluster, TimeSpan timeout, string? differentFrom = null)
    {
        string? lastLeader = null;
        await WaitUntilAsync(async () =>
        {
            var leaders = new HashSet<string>();
            foreach (var c in cluster)
            {
                var s = await TryGetStatusAsync(c.Config.Endpoint);
                if (s == null) continue;
                if (s.Role == "Leader") leaders.Add(s.NodeId);
            }
            if (leaders.Count == 1)
            {
                var l = leaders.First();
                if (differentFrom != null && l == differentFrom) return false;
                lastLeader = l;
                return true;
            }
            return false;
        }, timeout, differentFrom == null ? "exactly one leader" : $"a leader other than {differentFrom}");
        return lastLeader!;
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> check, TimeSpan timeout, string label)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            try { if (await check()) return; } catch { }
            await Task.Delay(500);
        }
        throw new TimeoutException($"Timeout waiting for: {label}");
    }

    private async Task<NodeStatus?> TryGetStatusAsync(string endpoint)
    {
        try
        {
            using var ch = GrpcChannel.ForAddress(endpoint);
            var client = MagicOnionClient.Create<IClusterService>(ch);
            var s = await client.GetStatusAsync();
            if (s != null) CheckInvariants(s);
            return s;
        }
        catch
        {
            return null;
        }
    }

    // -----------------------------------------------------------------------
    // Unix signal helpers (no-ops on Windows; tests fall back to kill)
    // -----------------------------------------------------------------------

    private static bool StopProcess(Process p) => SendUnixSignal(p, 19); // SIGSTOP
    private static bool ContinueProcess(Process p) => SendUnixSignal(p, 18); // SIGCONT

    private static bool SendUnixSignal(Process p, int signal)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) return false;
        try
        {
            int rc = NativeKill(p.Id, signal);
            return rc == 0;
        }
        catch { return false; }
    }

    [DllImport("libc", EntryPoint = "kill", SetLastError = true)]
    private static extern int NativeKill(int pid, int sig);

    private static void Log(string msg) =>
        Console.WriteLine($"[{DateTimeOffset.UtcNow:HH:mm:ss.fff} coordinator] {msg}");

    private sealed class NodeProcess
    {
        public NodeConfiguration Config { get; }
        public Process Process { get; }
        public NodeProcess(NodeConfiguration cfg, Process p) { Config = cfg; Process = p; }
    }
}
