using System;
using System.Threading;
using System.Threading.Tasks;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// </summary>
    public interface ISimpleStoreAsync : IDisposable
    {
        /// <summary>
        /// Various counts and other statistics.
        /// </summary>
        Task<Stats> GetStatsAsync(CancellationToken ct);

        /// <summary>
        /// </summary>
        Task AddAsync(string id, object value, Func<byte[]> getEncodedValue, CancellationToken ct);

        /// <summary>
        /// </summary>
        Task<byte[]> GetAsync(string id, CancellationToken ct);

        /// <summary>
        /// </summary>
        Task RemoveAsync(string id, CancellationToken ct);

        /// <summary>
        /// </summary>
        Task<object> TryGetFromCacheAsync(string id, CancellationToken ct);

        /// <summary>
        /// </summary>
        Task FlushAsync(CancellationToken ct);
    }
}
