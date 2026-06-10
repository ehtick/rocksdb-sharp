using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;

namespace RocksDbSharp
{
    /// <summary>
    /// Inventory entry describing one database file on either side of a
    /// snapshot transfer.
    /// </summary>
    public class ReplicationFileInfo
    {
        public string FileName { get; set; }
        public long Size { get; set; }
        /// <summary>SHA-256 of the file content (hex). Empty for files that are never reused.</summary>
        public string Hash { get; set; }
    }

    /// <summary>
    /// Result of comparing a source checkpoint manifest against a consumer's
    /// local inventory.
    /// </summary>
    public class ReplicationDeltaPlan
    {
        /// <summary>Files the consumer already holds byte-identically; not transferred.</summary>
        public List<string> FilesToReuse { get; } = new List<string>();
        /// <summary>Files that are new or changed and must be transferred in full.</summary>
        public List<string> FilesToTransfer { get; } = new List<string>();
    }

    /// <summary>
    /// Per-file delta computation for snapshot transfers: a consumer offers an
    /// inventory of its local immutable files, the source compares it against
    /// its checkpoint and only streams the files that are new or changed; the
    /// consumer deletes everything else before restoring.
    ///
    /// Granularity is whole files: a changed file is re-transferred in full,
    /// there is no content-level patching. Only immutable files (.sst / .blob)
    /// are ever reused. Matching requires name + size + SHA-256, because two
    /// RocksDB instances allocate file numbers independently once they stop
    /// sharing a lineage - files with equal names (and even equal sizes) on
    /// different nodes are not guaranteed to hold the same content.
    /// </summary>
    public static class ReplicationDelta
    {
        public static bool IsImmutableFile(string fileName) =>
            fileName.EndsWith(".sst", StringComparison.OrdinalIgnoreCase) ||
            fileName.EndsWith(".blob", StringComparison.OrdinalIgnoreCase);

        public static string ComputeFileHash(string path)
        {
            using (var sha = SHA256.Create())
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                var hash = sha.ComputeHash(fs);
                return BitConverter.ToString(hash).Replace("-", string.Empty);
            }
        }

        /// <summary>
        /// Scans the top level of a database directory for immutable files that
        /// are candidates for reuse in a delta transfer.
        /// </summary>
        public static List<ReplicationFileInfo> ScanImmutableFiles(string directory)
        {
            var result = new List<ReplicationFileInfo>();
            if (!Directory.Exists(directory)) return result;
            foreach (var filePath in Directory.GetFiles(directory))
            {
                var info = new FileInfo(filePath);
                if (!IsImmutableFile(info.Name)) continue;
                result.Add(new ReplicationFileInfo
                {
                    FileName = info.Name,
                    Size = info.Length,
                    Hash = ComputeFileHash(filePath),
                });
            }
            return result;
        }

        /// <summary>
        /// Splits the source manifest into files the consumer can keep and
        /// files that must be transferred.
        /// </summary>
        public static ReplicationDeltaPlan Compute(
            IEnumerable<ReplicationFileInfo> sourceManifest,
            IEnumerable<ReplicationFileInfo> consumerInventory)
        {
            var byName = new Dictionary<string, ReplicationFileInfo>(StringComparer.Ordinal);
            if (consumerInventory != null)
            {
                foreach (var f in consumerInventory)
                {
                    if (f?.FileName != null) byName[f.FileName] = f;
                }
            }

            var plan = new ReplicationDeltaPlan();
            foreach (var src in sourceManifest)
            {
                bool reusable = IsImmutableFile(src.FileName)
                                && byName.TryGetValue(src.FileName, out var local)
                                && local.Size == src.Size
                                && !string.IsNullOrEmpty(src.Hash)
                                && string.Equals(local.Hash, src.Hash, StringComparison.OrdinalIgnoreCase);
                if (reusable) plan.FilesToReuse.Add(src.FileName);
                else plan.FilesToTransfer.Add(src.FileName);
            }
            return plan;
        }

        /// <summary>
        /// Brings a database directory into the state the restore expects:
        /// every subdirectory (most importantly the WAL/journal directory) and
        /// every top-level file that is not reused is deleted, so files absent
        /// from the source - stale SSTs, the old MANIFEST/CURRENT, divergent
        /// WAL segments - cannot leak into the restored database. Entries
        /// named in <paramref name="preserveNames"/> are kept regardless.
        /// </summary>
        public static void PrepareForRestore(string dbPath, ICollection<string> filesToReuse, ICollection<string> preserveNames)
        {
            var keep = new HashSet<string>(filesToReuse ?? (ICollection<string>)Array.Empty<string>(), StringComparer.Ordinal);
            var preserve = new HashSet<string>(preserveNames ?? (ICollection<string>)Array.Empty<string>(), StringComparer.OrdinalIgnoreCase);

            foreach (var dir in Directory.GetDirectories(dbPath))
            {
                if (preserve.Contains(Path.GetFileName(dir))) continue;
                Directory.Delete(dir, true);
            }
            foreach (var file in Directory.GetFiles(dbPath))
            {
                var name = Path.GetFileName(file);
                if (preserve.Contains(name) || keep.Contains(name)) continue;
                File.Delete(file);
            }
        }
    }
}
