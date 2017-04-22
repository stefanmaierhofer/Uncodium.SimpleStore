using System;
using System.Threading;
using System.Threading.Tasks;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// Each operation may fail with a given probability. 
    /// </summary>
    public class WrapperRandomFailAsync : ISimpleStoreAsync
    {
        private Random m_random = new Random();
        private ISimpleStoreAsync m_store;
        private double m_pStats;
        private double m_pAdd;
        private double m_pGet;
        private double m_pRemove;
        private double m_pTryGetFromCache;
        private double m_pFlush;

        /// <summary>
        /// </summary>
        public WrapperRandomFailAsync(ISimpleStoreAsync store,
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
        public WrapperRandomFailAsync(ISimpleStoreAsync store, double pFail) : this(store, pFail, pFail, pFail, pFail, pFail, pFail)
        { }

        /// <summary>
        /// </summary>
        public Task<Stats> GetStatsAsync(CancellationToken ct)
            => m_random.NextDouble() < m_pStats ? throw new Exception() : m_store.GetStatsAsync(ct);

        /// <summary>
        /// </summary>
        public Task AddAsync(string id, object value, Func<byte[]> getEncodedValue, CancellationToken ct)
            => m_random.NextDouble() < m_pAdd ? throw new Exception() : m_store.AddAsync(id, value, getEncodedValue, ct);

        /// <summary>
        /// </summary>
        public Task<byte[]> GetAsync(string id, CancellationToken ct)
            => m_random.NextDouble() < m_pGet ? throw new Exception() : m_store.GetAsync(id, ct);

        /// <summary>
        /// </summary>
        public Task RemoveAsync(string id, CancellationToken ct)
            => m_random.NextDouble() < m_pRemove ? throw new Exception() : m_store.RemoveAsync(id, ct);

        /// <summary>
        /// </summary>
        public Task<object> TryGetFromCacheAsync(string id, CancellationToken ct)
            => m_random.NextDouble() < m_pTryGetFromCache ? throw new Exception() : m_store.TryGetFromCacheAsync(id, ct);

        /// <summary>
        /// </summary>
        public Task FlushAsync(CancellationToken ct)
            => m_random.NextDouble() < m_pFlush ? throw new Exception() : m_store.FlushAsync(ct);

        /// <summary>
        /// </summary>
        public void Dispose() => m_store.Dispose();
    }
}
