using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

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
        /// <summary>Identity signature (see <see cref="ReplicationDelta.ComputeFileSignatureAsync"/>). Empty for files that are never reused.</summary>
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
    /// are ever reused. Matching requires name + size + identity signature,
    /// because two RocksDB instances allocate file numbers independently once
    /// they stop sharing a lineage - files with equal names (and even equal
    /// sizes) on different nodes are not guaranteed to hold the same content.
    /// </summary>
    public static class ReplicationDelta
    {
        /// <summary>
        /// How much of the end of a file the signature covers. The tail of an
        /// SST holds the footer, the (top-level) index and the properties
        /// block - which embeds RocksDB's own identity material: the creating
        /// DB's UUID, the DB session identity (unique per instance run), the
        /// original file number and the creation timestamps. Two SSTs from
        /// different creation events therefore always differ inside this
        /// window, while byte-copies are identical everywhere. 1 MiB leaves
        /// ample margin for the index/metaindex blocks of large files.
        /// </summary>
        public const int SignatureTailBytes = 1024 * 1024;

        public static bool IsImmutableFile(string fileName) =>
            fileName.EndsWith(".sst", StringComparison.OrdinalIgnoreCase) ||
            fileName.EndsWith(".blob", StringComparison.OrdinalIgnoreCase);

        /// <summary>
        /// Identity signature of an immutable file: SHA-256 over the file
        /// length and the last <paramref name="tailBytes"/> bytes.
        ///
        /// Hashing the *head* instead would not be safe: a replica's locally
        /// flushed SST can carry a byte-identical data prefix (same keys,
        /// values and sequence numbers ingested from the WAL stream) and only
        /// differ near the end, where the identity properties live. Hashing
        /// the tail compares exactly that identity material - it is the same
        /// discriminator RocksDB's "unique SST id" is built from - without
        /// reading multi-gigabyte files in full. RocksDB itself stores no
        /// whole-file hash inside the SST (blocks carry individual CRCs, which
        /// still guard reused files against corruption at read time).
        /// </summary>
        public static async Task<string> ComputeFileSignatureAsync(
            string path,
            int tailBytes = SignatureTailBytes,
            CancellationToken cancellationToken = default)
        {
            using (var sha = SHA256.Create())
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite,
                                           bufferSize: 64 * 1024,
                                           FileOptions.Asynchronous | FileOptions.SequentialScan))
            {
                var header = BitConverter.GetBytes(fs.Length);
                sha.TransformBlock(header, 0, header.Length, null, 0);

                long start = Math.Max(0, fs.Length - tailBytes);
                fs.Seek(start, SeekOrigin.Begin);
                var buffer = new byte[64 * 1024];
                int read;
                while ((read = await fs.ReadAsync(buffer, 0, buffer.Length, cancellationToken).ConfigureAwait(false)) > 0)
                {
                    sha.TransformBlock(buffer, 0, read, null, 0);
                }
                sha.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
                return BitConverter.ToString(sha.Hash).Replace("-", string.Empty);
            }
        }

        /// <summary>
        /// Scans the top level of a database directory for immutable files that
        /// are candidates for reuse in a delta transfer. Hashes are not
        /// computed here: they are only worth computing for files whose name
        /// and size match on both sides, so callers verify candidates through
        /// a hash-request exchange (<see cref="ComputeFileSignatureAsync"/>) afterwards.
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
                    Hash = string.Empty,
                });
            }
            return result;
        }

        /// <summary>
        /// Hash-request helper for the source side: returns the hash of the
        /// named file only when it exists in <paramref name="directory"/>, is
        /// an immutable file, and matches <paramref name="expectedSize"/> -
        /// otherwise null, without computing anything. Immutability is what
        /// makes hashing the live file (rather than a checkpoint copy) valid.
        /// </summary>
        public static async Task<string> TryHashCandidateAsync(
            string directory, string fileName, long expectedSize, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(fileName) || Path.GetFileName(fileName) != fileName) return null;
            if (!IsImmutableFile(fileName)) return null;
            var path = Path.Combine(directory, fileName);
            var info = new FileInfo(path);
            if (!info.Exists || info.Length != expectedSize) return null;
            return await ComputeFileSignatureAsync(path, SignatureTailBytes, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Splits the source manifest into files the consumer can keep and
        /// files that must be transferred. Reuse requires a name + size match
        /// and, when both sides supply a content hash, agreeing hashes. When
        /// hashes are omitted the consumer must only list files it has already
        /// verified through the hash-request exchange: name + size alone do
        /// not imply equal content, because two RocksDB instances allocate
        /// file numbers independently once they stop sharing a lineage.
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
                                && (string.IsNullOrEmpty(src.Hash)
                                    || string.IsNullOrEmpty(local.Hash)
                                    || string.Equals(local.Hash, src.Hash, StringComparison.OrdinalIgnoreCase));
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
