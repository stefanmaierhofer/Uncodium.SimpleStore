using System;
using System.Threading;
using System.Threading.Tasks;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// Each operation will be delayed. 
    /// </summary>
    public class WrapperRandomDelayAsync : ISimpleStoreAsync
    {
        private Random m_random = new Random();
        private ISimpleStoreAsync m_store;
        private double m_dtStats;
        private double m_dtAdd;
        private double m_dtGet;
        private double m_dtRemove;
        private double m_dtTryGetFromCache;
        private double m_dtFlush;

        /// <summary>
        /// </summary>
        public WrapperRandomDelayAsync(ISimpleStoreAsync store,
            double dtStats, double dtAdd, double dtGet, double dtRemove, double dtTryGetFromCache, double dtFlush
            )
        {
            m_store = store ?? throw new ArgumentNullException(nameof(store));
            m_dtStats = dtStats;
            m_dtAdd = dtAdd;
            m_dtGet = dtGet;
            m_dtRemove = dtRemove;
            m_dtTryGetFromCache = dtTryGetFromCache;
            m_dtFlush = dtFlush;
        }

        /// <summary>
        /// </summary>
        public WrapperRandomDelayAsync(ISimpleStoreAsync store, double dt) : this(store, dt, dt, dt, dt, dt, dt)
        { }

        /// <summary>
        /// </summary>
        public async Task<Stats> GetStatsAsync(CancellationToken ct)
        {
            await Task.Delay(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtStats), ct);
            return await m_store.GetStatsAsync(ct);
        }

        /// <summary>
        /// </summary>
        public async Task AddAsync(string id, object value, Func<byte[]> getEncodedValue, CancellationToken ct)
        {
            await Task.Delay(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtAdd), ct);
            await m_store.AddAsync(id, value, getEncodedValue, ct);
        }

        /// <summary>
        /// </summary>
        public async Task<byte[]> GetAsync(string id, CancellationToken ct)
        {
            await Task.Delay(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtGet), ct);
            return await m_store.GetAsync(id, ct);
        }

        /// <summary>
        /// </summary>
        public async Task RemoveAsync(string id, CancellationToken ct)
        {
            await Task.Delay(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtRemove), ct);
            await m_store.RemoveAsync(id, ct);
        }

        /// <summary>
        /// </summary>
        public async Task<object> TryGetFromCacheAsync(string id, CancellationToken ct)
        {
            await Task.Delay(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtTryGetFromCache), ct);
            return await m_store.TryGetFromCacheAsync(id, ct);
        }

        /// <summary>
        /// </summary>
        public async Task FlushAsync(CancellationToken ct)
        {
            await Task.Delay(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtFlush), ct);
            await m_store.FlushAsync(ct);
        }

        /// <summary>
        /// </summary>
        public void Dispose() => m_store.Dispose();
    }
}
