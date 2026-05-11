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
            await new Coordinator().RunAsync(args.Skip(1).ToArray());
            return 0;
        }

        if (args[0] == "node")
        {
            string cfgPath = args[1];
            var cfg = JsonSerializer.Deserialize<NodeConfiguration>(await File.ReadAllTextAsync(cfgPath))
                      ?? throw new Exception("Bad config file");

            var host = new ClusterNodeHost(cfg);
            await host.StartAsync();

            // The "writer" role drives traffic when this node is the leader.
            _ = Task.Run(async () =>
            {
                long counter = 0;
                while (true)
                {
                    if (host.Node.Role == RocksDbSharp.RaftRole.Leader)
                    {
                        for (int i = 0; i < 200; i++)
                        {
                            string key = $"{host.Node.NodeId}_{counter++:D012}";
                            host.Db.Put(key, $"{Stopwatch.GetTimestamp()}");
                        }
                    }
                    await Task.Delay(50);
                }
            });

            // Console reporting once per second
            _ = Task.Run(async () =>
            {
                while (true)
                {
                    Console.WriteLine($"[{DateTimeOffset.UtcNow:HH:mm:ss.fff} {host.Node.NodeId}] " +
                                      $"role={host.Node.Role} term={host.Node.CurrentTerm} " +
                                      $"leader={host.Node.CurrentLeaderId} " +
                                      $"latest={host.Db.GetLatestSequenceNumber():n0} " +
                                      $"commit={host.Node.CommitSeq:n0} mode={host.Node.Mode} " +
                                      $"bootSync={host.Node.BootstrapInSyncCount}");
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

    public async Task RunAsync(string[] args)
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
                    return;
            }
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
        var cluster = StartCluster(3, scenarioName: "election");

        await WaitForClusterModeAsync(cluster, TimeSpan.FromSeconds(45));
        Log("cluster has switched to Cluster mode");

        var leader = await WaitForLeaderAsync(cluster, TimeSpan.FromSeconds(20));
        Log($"current leader = {leader}");

        KillAll();
        await Task.Delay(500);
    }

    private async Task ScenarioLeaderCrashAsync()
    {
        Log("=== scenario: leader crash → re-election ===");
        var cluster = StartCluster(3, scenarioName: "crash");
        await WaitForClusterModeAsync(cluster, TimeSpan.FromSeconds(45));
        var leader = await WaitForLeaderAsync(cluster, TimeSpan.FromSeconds(20));
        Log($"initial leader = {leader}");

        var leaderProc = cluster.First(c => c.Config.NodeId == leader);
        Log($"killing leader {leader} (pid {leaderProc.Process.Id})");
        leaderProc.Process.Kill(entireProcessTree: true);
        await Task.Delay(500);

        // remaining nodes should elect a new leader
        var remaining = cluster.Where(c => c.Config.NodeId != leader).ToList();
        var newLeader = await WaitForLeaderAsync(remaining, TimeSpan.FromSeconds(30), differentFrom: leader);
        Log($"new leader = {newLeader}");

        KillAll();
        await Task.Delay(500);
    }

    private async Task ScenarioLeaderHangAsync()
    {
        Log("=== scenario: leader hang (SIGSTOP) → re-election → SIGCONT ===");
        var cluster = StartCluster(3, scenarioName: "hang");
        await WaitForClusterModeAsync(cluster, TimeSpan.FromSeconds(45));
        var leader = await WaitForLeaderAsync(cluster, TimeSpan.FromSeconds(20));
        Log($"initial leader = {leader}");

        var leaderProc = cluster.First(c => c.Config.NodeId == leader);
        Log($"freezing leader {leader} (pid {leaderProc.Process.Id})");
        if (!StopProcess(leaderProc.Process))
        {
            Log("SIGSTOP unavailable on this OS; falling back to Kill");
            leaderProc.Process.Kill(entireProcessTree: true);
        }

        // followers should elect a new leader once the heartbeat lapses
        var remaining = cluster.Where(c => c.Config.NodeId != leader).ToList();
        var newLeader = await WaitForLeaderAsync(remaining, TimeSpan.FromSeconds(30), differentFrom: leader);
        Log($"new leader (while old is frozen) = {newLeader}");

        // resume the frozen node and confirm it becomes a follower of the new leader
        Log($"resuming {leader}");
        ContinueProcess(leaderProc.Process);

        await WaitUntilAsync(async () =>
        {
            var s = await TryGetStatusAsync(leaderProc.Config.Endpoint);
            return s != null && s.LeaderId == newLeader && s.Role == "Follower";
        }, TimeSpan.FromSeconds(20), $"resumed node {leader} accepts {newLeader} as leader");

        KillAll();
        await Task.Delay(500);
    }

    private async Task ScenarioFollowerCrashRejoinAsync()
    {
        Log("=== scenario: follower crash → restart → catch-up ===");
        var cluster = StartCluster(3, scenarioName: "rejoin");
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
            if (s == null) return false;
            var leaderStatus = await TryGetStatusAsync(cluster.First(c => c.Config.NodeId == leader).Config.Endpoint);
            if (leaderStatus == null) return false;
            return s.LatestSeq + 5000 >= leaderStatus.LatestSeq;
        }, TimeSpan.FromSeconds(60), $"follower {follower.Config.NodeId} caught back up");

        KillAll();
        await Task.Delay(500);
    }

    // -----------------------------------------------------------------------
    // Cluster bootstrap helpers
    // -----------------------------------------------------------------------

    private List<NodeProcess> StartCluster(int nodeCount, string scenarioName)
    {
        int basePort = 51000 + (Math.Abs(scenarioName.GetHashCode()) % 50) * 10;
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

    private static async Task<NodeStatus?> TryGetStatusAsync(string endpoint)
    {
        try
        {
            using var ch = GrpcChannel.ForAddress(endpoint);
            var client = MagicOnionClient.Create<IClusterService>(ch);
            return await client.GetStatusAsync();
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
