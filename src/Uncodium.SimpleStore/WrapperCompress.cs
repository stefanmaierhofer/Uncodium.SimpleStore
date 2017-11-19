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
        private ISimpleStore m_store;
        private CompressionLevel m_compressionLevel;

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
        public void Add(string id, object value, Func<byte[]> getEncodedValue)
        {
            if (getEncodedValue == null)
            {
                m_store.Add(id, value, getEncodedValue);
                return;
            }

            Func<byte[]> f = () =>
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
            };
            m_store.Add(id, value, f);
        }

        /// <summary>
        /// </summary>
        public byte[] Get(string id)
        {
            var buffer = m_store.Get(id);
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
        public void Remove(string id)
        {
            m_store.Remove(id);
        }

        /// <summary>
        /// </summary>
        public object TryGetFromCache(string id)
        {
            return m_store.TryGetFromCache(id);
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
