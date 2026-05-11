#if NET5_0_OR_GREATER
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace RocksDbSharp;

/// <summary>
/// Sparse map of "log entry term" for the WAL. Each new term records the
/// starting WAL sequence number at which it became active. To look up the
/// term of any sequence number we binary-search for the latest range that
/// starts at or before it.
/// </summary>
public sealed class RaftLogTerms
{
    // sorted by StartSeq ascending
    private readonly List<(ulong StartSeq, long Term)> _ranges = new();
    private readonly object _gate = new();

    public void Record(ulong startSeq, long term)
    {
        lock (_gate)
        {
            // If this term already started earlier, ignore - we only record the
            // first sequence number of each term.
            if (_ranges.Count > 0)
            {
                var last = _ranges[^1];
                if (last.Term == term) return;
                if (last.StartSeq > startSeq)
                    throw new InvalidOperationException(
                        $"Term ranges must be monotonic; new range starts at {startSeq} but last starts at {last.StartSeq}");
            }
            _ranges.Add((startSeq, term));
        }
    }

    /// <summary>
    /// Returns the term assigned to the WAL entry at <paramref name="seq"/>.
    /// Returns 0 if there are no recorded ranges or seq is before the first.
    /// </summary>
    public long GetTermForSeq(ulong seq)
    {
        lock (_gate)
        {
            if (_ranges.Count == 0) return 0;
            // binary search for the largest StartSeq <= seq
            int lo = 0, hi = _ranges.Count - 1, idx = -1;
            while (lo <= hi)
            {
                int mid = (lo + hi) >> 1;
                if (_ranges[mid].StartSeq <= seq)
                {
                    idx = mid;
                    lo = mid + 1;
                }
                else
                {
                    hi = mid - 1;
                }
            }
            return idx < 0 ? 0 : _ranges[idx].Term;
        }
    }

    public (ulong StartSeq, long Term) GetLast()
    {
        lock (_gate)
        {
            return _ranges.Count == 0 ? (0UL, 0L) : _ranges[^1];
        }
    }

    public string Serialize()
    {
        lock (_gate)
        {
            var sb = new StringBuilder();
            foreach (var (s, t) in _ranges) sb.Append(s).Append(',').Append(t).Append('\n');
            return sb.ToString();
        }
    }

    public static RaftLogTerms Deserialize(string content)
    {
        var t = new RaftLogTerms();
        if (string.IsNullOrEmpty(content)) return t;
        foreach (var line in content.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = line.Split(',');
            if (parts.Length != 2) continue;
            if (ulong.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out var s) &&
                long.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var term))
            {
                t._ranges.Add((s, term));
            }
        }
        return t;
    }
}
#endif
