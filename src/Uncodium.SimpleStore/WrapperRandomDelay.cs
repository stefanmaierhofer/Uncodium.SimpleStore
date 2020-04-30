using System;
using System.Threading;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// Each operation will be delayed. 
    /// </summary>
    public class WrapperRandomDelay : ISimpleStore
    {
        private readonly Random m_random = new Random();
        private readonly ISimpleStore m_store;
        private readonly double m_dtStats;
        private readonly double m_dtAdd;
        private readonly double m_dtContains;
        private readonly double m_dtGet;
        private readonly double m_dtRemove;
        private readonly double m_dtTryGetFromCache;
        private readonly double m_dtFlush;

        /// <summary>
        /// </summary>
        public WrapperRandomDelay(ISimpleStore store,
            double dtStats, double dtAdd, double dtContains, double dtGet, double dtRemove, double dtTryGetFromCache, double dtFlush
            )
        {
            m_store = store ?? throw new ArgumentNullException(nameof(store));
            m_dtStats = dtStats;
            m_dtAdd = dtAdd;
            m_dtContains = dtContains;
            m_dtGet = dtGet;
            m_dtRemove = dtRemove;
            m_dtTryGetFromCache = dtTryGetFromCache;
            m_dtFlush = dtFlush;
        }

        /// <summary>
        /// </summary>
        public WrapperRandomDelay(ISimpleStore store, double dt) : this(store, dt, dt, dt, dt, dt, dt, dt)
        { }

        /// <summary>
        /// </summary>
        public Stats Stats
        {
            get
            {
                Thread.Sleep(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtStats));
                return m_store.Stats;
            }
        }

        /// <summary>
        /// </summary>
        public void Add(string key, object value, Func<byte[]> getEncodedValue)
        {
            Thread.Sleep(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtAdd));
            m_store.Add(key, value, getEncodedValue);
        }

        /// <summary>
        /// </summary>
        public bool Contains(string key)
        {
            Thread.Sleep(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtContains));
            return m_store.Contains(key);
        }

        /// <summary>
        /// </summary>
        public byte[] Get(string key)
        {
            Thread.Sleep(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtGet));
            return m_store.Get(key);
        }

        /// <summary>
        /// </summary>
        public void Remove(string key)
        {
            Thread.Sleep(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtRemove));
            m_store.Remove(key);
        }

        /// <summary>
        /// </summary>
        public object TryGetFromCache(string key)
        {
            Thread.Sleep(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtTryGetFromCache));
            return m_store.TryGetFromCache(key);
        }

        /// <summary>
        /// </summary>
        public string[] SnapshotKeys() => m_store.SnapshotKeys();

        /// <summary>
        /// </summary>
        public void Flush()
        {
            Thread.Sleep(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtFlush));
            m_store.Flush();
        }

        /// <summary>
        /// </summary>
        public void Dispose() => m_store.Dispose();
    }
}
