using System;
using System.Collections.Generic;
using System.IO;

namespace RocksDbSharp
{
    public class ReplicationFile : IDisposable
    {
        public string FileName { get; set; }
        public ulong FileSize { get; set; }
        public Stream FileStream { get; set; }

        public void Dispose()
        {
            FileStream?.Dispose();
        }
    }

    public class ReplicationBatch
    {
        public ulong SequenceNumber { get; set; }
        public byte[] Data { get; set; }
    }

    public class PooledReplicationBatch
    {
        public ulong SequenceNumber { get; set; }
        public byte[] PooledData { get; set; }
        public int Length { get; set; }
    }

    public class ReplicationSession : IDisposable
    {
        private readonly string _tempPath;

        public ReplicationSession(string tempPath)
        {
            _tempPath = tempPath;
        }

        public IEnumerable<ReplicationFile> Files
        {
            get
            {
                foreach (var filePath in Directory.GetFiles(_tempPath))
                {
                    yield return OpenFile(Path.GetFileName(filePath));
                }
            }
        }

        /// <summary>
        /// Lists the checkpoint's files with size and, for immutable files,
        /// a content hash - the source-side half of a per-file delta transfer.
        /// </summary>
        public List<ReplicationFileInfo> GetManifest()
        {
            var result = new List<ReplicationFileInfo>();
            foreach (var filePath in Directory.GetFiles(_tempPath))
            {
                var fileInfo = new FileInfo(filePath);
                result.Add(new ReplicationFileInfo
                {
                    FileName = fileInfo.Name,
                    Size = fileInfo.Length,
                    Hash = ReplicationDelta.IsImmutableFile(fileInfo.Name)
                        ? ReplicationDelta.ComputeFileHash(filePath)
                        : string.Empty,
                });
            }
            return result;
        }

        public ReplicationFile OpenFile(string fileName)
        {
            var filePath = Path.Combine(_tempPath, fileName);
            var fileInfo = new FileInfo(filePath);
            var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            return new ReplicationFile
            {
                FileName = fileName,
                FileSize = (ulong)fileInfo.Length,
                FileStream = stream
            };
        }

        public void Dispose()
        {
            if (Directory.Exists(_tempPath))
            {
                Directory.Delete(_tempPath, true);
            }
        }
    }
}
