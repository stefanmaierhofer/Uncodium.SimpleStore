using System;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// Each operation may fail with a given probability. 
    /// </summary>
    public class WrapperRandomFail : ISimpleStore
    {
        private Random m_random = new Random();
        private ISimpleStore m_store;
        private double m_pStats;
        private double m_pAdd;
        private double m_pGet;
        private double m_pRemove;
        private double m_pTryGetFromCache;
        private double m_pFlush;

        /// <summary>
        /// </summary>
        public WrapperRandomFail(ISimpleStore store,
            double pStats, double pAdd, double pGet, double pRemove, double pTryGetFromCache, double pFlush
            )
        {
            m_store = store ?? throw new ArgumentNullException(nameof(store));
            m_pStats = pStats;
            m_pAdd = pAdd;
            m_pGet = pGet;
            m_pRemove = pRemove;
            m_pTryGetFromCache = pTryGetFromCache;
            m_pFlush = pFlush;
        }

        /// <summary>
        /// </summary>
        public WrapperRandomFail(ISimpleStore store, double pFail) : this(store, pFail, pFail, pFail, pFail, pFail, pFail)
        { }

        /// <summary>
        /// </summary>
        public Stats Stats
            => m_random.NextDouble() < m_pStats ? throw new Exception() : m_store.Stats;

        /// <summary>
        /// </summary>
        public void Add(string id, object value, Func<byte[]> getEncodedValue)
        {
            if (m_random.NextDouble() < m_pAdd) throw new Exception();
            m_store.Add(id, value, getEncodedValue);
        }

        /// <summary>
        /// </summary>
        public byte[] Get(string id)
            => m_random.NextDouble() < m_pGet ? throw new Exception() : m_store.Get(id);

        /// <summary>
        /// </summary>
        public void Remove(string id)
        {
            if (m_random.NextDouble() < m_pRemove) throw new Exception();
            m_store.Remove(id);
        }

        /// <summary>
        /// </summary>
        public object TryGetFromCache(string id)
            => m_random.NextDouble() < m_pTryGetFromCache ? throw new Exception() : m_store.TryGetFromCache(id);

        /// <summary>
        /// </summary>
        public void Flush()
        {
            if (m_random.NextDouble() < m_pFlush) throw new Exception();
            m_store.Flush();
        }

        /// <summary>
        /// </summary>
        public void Dispose() => m_store.Dispose();
    }
}
