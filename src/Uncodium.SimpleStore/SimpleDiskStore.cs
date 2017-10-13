using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// A memory-mapped key/value store on disk.
    /// </summary>
    public class SimpleDiskStore : ISimpleStore
    {
        private string m_dbDiskLocation;
        private string m_indexFilename;
        private string m_dataFilename;

        private Dictionary<string, Tuple<long, int>> m_dbIndex;
        private Dictionary<string, WeakReference<object>> m_dbCache;
        private HashSet<object> m_dbCacheKeepAlive;
        private HashSet<Type> m_typesToKeepAlive;
        private long m_dataSize;
        private long m_dataPos;
        private MemoryMappedFile m_mmf;
        private MemoryMappedViewAccessor m_accessorSize;
        private MemoryMappedViewAccessor m_accessor;

        private Stats m_stats;

        private bool m_indexHasChanged = false;
        private bool m_isDisposed = false;
        private CancellationTokenSource m_cts = new CancellationTokenSource();

        /// <summary>
        /// Creates store in folder 'dbDiskLocation'.
        /// Optional set of types that will be kept alive in memory.
        /// </summary>
        public SimpleDiskStore(string dbDiskLocation, Type[] typesToKeepAlive)
        {
            m_dbDiskLocation = dbDiskLocation;

            if (typesToKeepAlive == null) typesToKeepAlive = new Type[0];
            m_typesToKeepAlive = new HashSet<Type>(typesToKeepAlive);

            Init();
        }

        /// <summary>
        /// Creates store in folder 'dbDiskLocation'.
        /// </summary>
        public SimpleDiskStore(string dbDiskLocation) : this(dbDiskLocation, null) { }
        
        private void Init()
        {
            m_indexFilename = Path.Combine(m_dbDiskLocation, "index.bin");
            m_dataFilename = Path.Combine(m_dbDiskLocation, "data.bin");
            var dataFileIsNewlyCreated = false;
            if (!Directory.Exists(m_dbDiskLocation)) Directory.CreateDirectory(m_dbDiskLocation);
            if (!File.Exists(m_dataFilename))
            {
                File.WriteAllBytes(m_dataFilename, new byte[0]);
                dataFileIsNewlyCreated = true;
            }
            
            m_dbCache = new Dictionary<string, WeakReference<object>>();
            m_dbCacheKeepAlive = new HashSet<object>();
            if (File.Exists(m_indexFilename))
            {
                using (var f = File.Open(m_indexFilename, FileMode.Open, FileAccess.Read, FileShare.None))
                using (var br = new BinaryReader(f))
                {
                    var count = br.ReadInt32();
                    m_dbIndex = new Dictionary<string, Tuple<long, int>>(count);
                    for (var i = 0; i < count; i++)
                    {
                        var key = br.ReadString();
                        var offset = br.ReadInt64();
                        var size = br.ReadInt32();
                        m_dbIndex[key] = Tuple.Create(offset, size);
                    }
                }
            }
            else
            {
                m_dbIndex = new Dictionary<string, Tuple<long, int>>();
            }

            m_dataSize = new FileInfo(m_dataFilename).Length;
            if (m_dataSize == 0) m_dataSize = 1024 * 1024; else m_dataSize -= 8;

            m_mmf = MemoryMappedFile.CreateFromFile(m_dataFilename, FileMode.OpenOrCreate, null, 8 + m_dataSize, MemoryMappedFileAccess.ReadWrite);
            m_accessorSize = m_mmf.CreateViewAccessor(0, 8);
            m_accessor = m_mmf.CreateViewAccessor(8, m_dataSize);

            m_dataPos = dataFileIsNewlyCreated ? 0L : m_accessorSize.ReadInt64(0);
        }

        /// <summary>
        /// Various counts and other statistics.
        /// </summary>
        public Stats Stats => m_stats;

        /// <summary>
        /// Adds key/value pair to store.
        /// If 'getEncodedValue' is null, than value will not be written to disk.
        /// </summary>
        public void Add(string key, object value, Func<byte[]> getEncodedValue)
        {
            if (m_isDisposed) throw new ObjectDisposedException("SimpleDiskStore");
            Interlocked.Increment(ref m_stats.CountAdd);

            byte[] buffer = null;
            lock (m_dbCache)
            {
                m_dbCache[key] = new WeakReference<object>(value);
                if (m_typesToKeepAlive.Contains(value.GetType()))
                {
                    m_dbCacheKeepAlive.Add(value);
                    Interlocked.Increment(ref m_stats.CountKeepAlive);
                }
                buffer = getEncodedValue?.Invoke();
            }

            if (buffer == null) return;

            lock (m_dbDiskLocation)
            {
                if (m_dataPos + buffer.Length > m_dataSize)
                {
                    if (m_dataSize < 1024 * 1024 * 1024)
                    {
                        m_dataSize *= 2;
                    }
                    else
                    {
                        m_dataSize += 1024 * 1024 * 1024;
                    }
                    Console.WriteLine($"[SimpleDiskStore] RESIZE to {m_dataSize / (1024 * 1024 * 1024.0):0.000} GiB");
                    m_accessorSize.Dispose(); m_accessor.Dispose(); m_mmf.Dispose();
                    m_mmf = MemoryMappedFile.CreateFromFile(m_dataFilename, FileMode.OpenOrCreate, null, 8 + m_dataSize, MemoryMappedFileAccess.ReadWrite);
                    m_accessorSize = m_mmf.CreateViewAccessor(0, 8);
                    m_accessor = m_mmf.CreateViewAccessor(8, m_dataSize);
                }
                m_accessor.WriteArray(m_dataPos, buffer, 0, buffer.Length);
                m_dbIndex[key] = Tuple.Create(m_dataPos, buffer.Length);
                m_indexHasChanged = true;
                m_dataPos += buffer.Length;
                m_accessorSize.Write(0, m_dataPos);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public byte[] Get(string id)
        {
            if (m_isDisposed) throw new ObjectDisposedException("SimpleDiskStore");
            lock (m_dbDiskLocation)
            {
                if (m_dbIndex.TryGetValue(id, out Tuple<long, int> entry))
                {
                    var buffer = new byte[entry.Item2];
                    var readcount = m_accessor.ReadArray(entry.Item1, buffer, 0, buffer.Length);
                    if (readcount != buffer.Length) throw new InvalidOperationException();
                    Interlocked.Increment(ref m_stats.CountGet);
                    return buffer;
                }
                else
                {
                    Interlocked.Increment(ref m_stats.CountGetInvalidKey);
                    return null;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Remove(string id)
        {
            if (m_isDisposed) throw new ObjectDisposedException("SimpleDiskStore");
            lock (m_dbDiskLocation)
            {
                m_dbIndex.Remove(id);
                m_indexHasChanged = true;
            }
            Interlocked.Increment(ref m_stats.CountRemove);
        }

        /// <summary>
        /// 
        /// </summary>
        public object TryGetFromCache(string id)
        {
            if (m_isDisposed) throw new ObjectDisposedException("SimpleDiskStore");
            lock (m_dbCache)
            {
                if (m_dbCache.TryGetValue(id, out WeakReference<object> weakRef))
                {
                    if (weakRef.TryGetTarget(out object data))
                    {
                        Interlocked.Increment(ref m_stats.CountGetCacheHit);
                        return data;
                    }
                }
                Interlocked.Increment(ref m_stats.CountGetCacheMiss);
                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Flush()
        {
            if (m_isDisposed) throw new ObjectDisposedException("DiskStorage");
            m_accessor.Flush();
            m_accessorSize.Flush();
            FlushIndex();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            try
            {
                if (m_isDisposed) throw new ObjectDisposedException("DiskStorage");
                m_accessor.Flush();
                m_accessor.Dispose();
                m_accessorSize.Flush();
                m_accessorSize.Dispose();
                m_mmf.Dispose();
                FlushIndex();
                m_cts.Cancel();
            }
            finally
            {
                m_isDisposed = true;
            }
        }

        private void FlushIndex()
        {
            if (!m_indexHasChanged) return;
            
            using (var f = File.Open(m_indexFilename, FileMode.Create, FileAccess.Write, FileShare.None))
            using (var bw = new BinaryWriter(f))
            {
                bw.Write(m_dbIndex.Count);
                foreach (var kv in m_dbIndex)
                {
                    bw.Write(kv.Key);
                    bw.Write(kv.Value.Item1);
                    bw.Write(kv.Value.Item2);
                }
            }
            m_indexHasChanged = false;
        }
    }
}
