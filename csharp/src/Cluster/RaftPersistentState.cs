#if NET5_0_OR_GREATER
using System;
using System.IO;
using System.Text;
using System.Threading;

namespace RocksDbSharp;

/// <summary>
/// Persists the bits of Raft state that must survive a process crash:
///  - currentTerm
///  - votedFor (in the current term)
///  - clusterMode (Bootstrap vs Cluster)
///  - the per-seqNo term log (so a recovering follower can answer
///    "what was the term of log entry X?")
///
/// Everything is written atomically through a temp-file + rename so a crash
/// mid-write cannot leave us with corrupt state.
/// </summary>
public sealed class RaftPersistentState
{
    private readonly string _statePath;
    private readonly string _termsPath;
    private readonly object _gate = new();

    private long _currentTerm;
    private string? _votedFor;
    private int _mode; // ClusterMode
    private RaftLogTerms _terms;

    public RaftPersistentState(string directory)
    {
        Directory.CreateDirectory(directory);
        _statePath = Path.Combine(directory, "raft.state");
        _termsPath = Path.Combine(directory, "raft.terms");
        _terms = new RaftLogTerms();
        Load();
    }

    public long CurrentTerm => Interlocked.Read(ref _currentTerm);

    public string? VotedFor
    {
        get { lock (_gate) return _votedFor; }
    }

    public ClusterMode Mode
    {
        get => (ClusterMode)Volatile.Read(ref _mode);
    }

    public RaftLogTerms Terms => _terms;

    public void SetMode(ClusterMode mode)
    {
        lock (_gate)
        {
            _mode = (int)mode;
            Persist();
        }
    }

    /// <summary>
    /// Atomically updates currentTerm and votedFor. If the new term is higher
    /// than the recorded term, votedFor is cleared unless a vote is being cast
    /// for this same term.
    /// </summary>
    public void UpdateTermAndVote(long newTerm, string? newVotedFor)
    {
        lock (_gate)
        {
            if (newTerm > _currentTerm)
            {
                _currentTerm = newTerm;
                _votedFor = newVotedFor;
            }
            else if (newTerm == _currentTerm && newVotedFor != null && _votedFor == null)
            {
                _votedFor = newVotedFor;
            }
            else
            {
                return;
            }
            Persist();
        }
    }

    /// <summary>
    /// Record the term of a log range starting at <paramref name="startSeq"/>.
    /// Persists immediately.
    /// </summary>
    public void RecordTermRange(ulong startSeq, long term)
    {
        lock (_gate)
        {
            _terms.Record(startSeq, term);
            File.WriteAllText(TempPath(_termsPath), _terms.Serialize());
            File.Move(TempPath(_termsPath), _termsPath, overwrite: true);
        }
    }

    private void Persist()
    {
        var sb = new StringBuilder();
        sb.Append(_currentTerm).Append('\n');
        sb.Append(_votedFor ?? string.Empty).Append('\n');
        sb.Append(_mode).Append('\n');

        string tmp = TempPath(_statePath);
        File.WriteAllText(tmp, sb.ToString());
        File.Move(tmp, _statePath, overwrite: true);
    }

    private void Load()
    {
        if (File.Exists(_statePath))
        {
            try
            {
                var lines = File.ReadAllLines(_statePath);
                if (lines.Length >= 1 && long.TryParse(lines[0], out var t)) _currentTerm = t;
                if (lines.Length >= 2) _votedFor = string.IsNullOrEmpty(lines[1]) ? null : lines[1];
                if (lines.Length >= 3 && int.TryParse(lines[2], out var m)) _mode = m;
            }
            catch
            {
                // corrupt file; start fresh
                _currentTerm = 0;
                _votedFor = null;
                _mode = 0;
            }
        }

        if (File.Exists(_termsPath))
        {
            try
            {
                _terms = RaftLogTerms.Deserialize(File.ReadAllText(_termsPath));
            }
            catch
            {
                _terms = new RaftLogTerms();
            }
        }
    }

    private static string TempPath(string path) => path + ".tmp";
}
#endif
