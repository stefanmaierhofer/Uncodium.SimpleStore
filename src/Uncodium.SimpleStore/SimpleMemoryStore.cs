using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// 
    /// </summary>
    public class SimpleMemoryStore : ISimpleStore
    {
        private Dictionary<string, Func<byte[]>> m_db;
        private Dictionary<string, object> m_dbCache;
        private Stats m_stats;

        /// <summary>
        /// 
        /// </summary>
        public SimpleMemoryStore()
        {
            m_db = new Dictionary<string, Func<byte[]>>();
            m_dbCache = new Dictionary<string, object>();
        }

        /// <summary>
        /// Various counts and other statistics.
        /// </summary>
        public Stats Stats => m_stats;

        /// <summary>
        /// </summary>
        public void Add(string key, object value, Func<byte[]> getEncodedValue = null)
        {
            lock (m_db)
            {
                m_db[key] = getEncodedValue;
                m_dbCache[key] = value;
            }
            Interlocked.Increment(ref m_stats.CountAdd);
        }

        /// <summary>
        /// </summary>
        public byte[] Get(string key)
        {
            lock (m_db)
            {
                if (m_db.TryGetValue(key, out Func<byte[]> f))
                {
                    Interlocked.Increment(ref m_stats.CountGet);
                    return f?.Invoke();
                }
                else
                {
                    Interlocked.Increment(ref m_stats.CountGetInvalidKey);
                    return null;
                }
            }
        }

        /// <summary>
        /// </summary>
        public void Remove(string key)
        {
            lock (m_db)
            {
                if (m_db.Remove(key))
                {
                    m_dbCache.Remove(key);
                    Interlocked.Increment(ref m_stats.CountRemove);
                }
                else
                {
                    Interlocked.Increment(ref m_stats.CountRemoveInvalidKey);
                }
            }
        }

        /// <summary>
        /// </summary>
        public object TryGetFromCache(string key)
        {
            lock (m_db)
            {
                if (m_dbCache.TryGetValue(key, out object value))
                {
                    Interlocked.Increment(ref m_stats.CountGetCacheHit);
                    return value;
                }
            }

            Interlocked.Increment(ref m_stats.CountGetCacheMiss);
            return null;
        }

        /// <summary>
        /// </summary>
        public string[] SnapshotKeys()
        {
            lock (m_db)
            {
                Interlocked.Increment(ref m_stats.CountSnapshotKeys);
                return m_db.Keys.ToArray();
            }
        }

        /// <summary>
        /// </summary>
        public void Flush()
        {
            Interlocked.Increment(ref m_stats.CountFlush);
        }

        /// <summary>
        /// </summary>
        public void Dispose()
        {
        }
    }
}
