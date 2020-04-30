using System;
using System.IO;
using System.IO.Compression;
using System.Threading;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// Compressed storage. 
    /// </summary>
    public class WrapperCompress : ISimpleStore
    {
        private readonly ISimpleStore m_store;
        private readonly CompressionLevel m_compressionLevel;

        /// <summary>
        /// </summary>
        public WrapperCompress(ISimpleStore store, CompressionLevel compressionLevel)
        {
            m_store = store ?? throw new ArgumentNullException(nameof(store));
            m_compressionLevel = compressionLevel;
        }

        /// <summary>
        /// </summary>
        public Stats Stats => m_store.Stats;

        /// <summary>
        /// </summary>
        public void Add(string key, object value, Func<byte[]> getEncodedValue)
        {
            if (getEncodedValue == null)
            {
                m_store.Add(key, value, getEncodedValue);
                return;
            }

            m_store.Add(key, value, f);

            byte[] f()
            {
                var ms = new MemoryStream();
                var data = getEncodedValue();
                using (var bw = new BinaryWriter(ms))
                {
                    bw.Write(data.Length);
                    using (var zs = new GZipStream(ms, m_compressionLevel, false))
                    {
                        zs.Write(data, 0, data.Length);
                    }
                }
                return ms.ToArray();
            }
        }

        /// <summary>
        /// </summary>
        public bool Contains(string key) => m_store.Contains(key);

        /// <summary>
        /// </summary>
        public byte[] Get(string key)
        {
            var buffer = m_store.Get(key);
            if (buffer == null) return null;
            var ms = new MemoryStream(buffer);
            using (var br = new BinaryReader(ms))
            {
                var count = br.ReadInt32();
                var data = new byte[count];
                using (var zs = new GZipStream(ms, CompressionMode.Decompress))
                {
                    var l = zs.Read(data, 0, count);
                    if (l != count) throw new Exception($"Read {l} bytes instead of {count} bytes.");
                    return data;
                }
            }
        }

        /// <summary>
        /// </summary>
        public void Remove(string key)
        {
            m_store.Remove(key);
        }

        /// <summary>
        /// </summary>
        public object TryGetFromCache(string key)
        {
            return m_store.TryGetFromCache(key);
        }

        /// <summary>
        /// </summary>
        public string[] SnapshotKeys() => m_store.SnapshotKeys();

        /// <summary>
        /// </summary>
        public void Flush()
        {
            m_store.Flush();
        }

        /// <summary>
        /// </summary>
        public void Dispose() => m_store.Dispose();
    }
}
