using System.Text;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// Store extensions. 
    /// </summary>
    public static class Extensions
    {
        /// <summary>
        /// String will be stored UTF8 encoded.
        /// </summary>
        public static void Add(this ISimpleStore store, string id, string value)
            => store.Add(id, value, () => Encoding.UTF8.GetBytes(value));

        /// <summary>
        /// Each store operation fails with given probability.
        /// </summary>
        public static ISimpleStore FailRandomly(this ISimpleStore store, double pFail)
            => new WrapperRandomFail(store, pFail);

        /// <summary>
        /// Each store operation fails with given probability.
        /// </summary>
        public static ISimpleStore FailRandomly(this ISimpleStore store, double pStats, double pAdd, double pGet, double pRemove, double pTryGetFromCache, double pFlush)
            => new WrapperRandomFail(store, pStats, pAdd, pGet, pRemove, pTryGetFromCache, pFlush);

        /// <summary>
        /// Each store operation is delayed between 0 and given duration in seconds.
        /// </summary>
        public static ISimpleStore DelayRandomly(this ISimpleStore store, double dt)
            => new WrapperRandomDelay(store, dt);

        /// <summary>
        /// Each store operation is delayed between 0 and given duration in seconds.
        /// </summary>
        public static ISimpleStore DelayRandomly(this ISimpleStore store, double dtStats, double dtAdd, double dtGet, double dtRemove, double dtTryGetFromCache, double dtFlush)
            => new WrapperRandomDelay(store, dtStats, dtAdd, dtGet, dtRemove, dtTryGetFromCache, dtFlush);

        /// <summary>
        /// Makes all store operations async.
        /// </summary>
        public static ISimpleStoreAsync Async(this ISimpleStore store)
            => new WrapperAsync(store);

        /// <summary>
        /// Each store operation fails with given probability.
        /// </summary>
        public static ISimpleStoreAsync FailRandomly(this ISimpleStoreAsync store, double pFail)
            => new WrapperRandomFailAsync(store, pFail);

        /// <summary>
        /// Each store operation fails with given probability.
        /// </summary>
        public static ISimpleStoreAsync FailRandomly(this ISimpleStoreAsync store, double pStats, double pAdd, double pGet, double pRemove, double pTryGetFromCache, double pFlush)
            => new WrapperRandomFailAsync(store, pStats, pAdd, pGet, pRemove, pTryGetFromCache, pFlush);

        /// <summary>
        /// Each store operation is delayed between 0 and given duration in seconds.
        /// </summary>
        public static ISimpleStoreAsync DelayRandomly(this ISimpleStoreAsync store, double dt)
            => new WrapperRandomDelayAsync(store, dt);

        /// <summary>
        /// Each store operation is delayed between 0 and given duration in seconds.
        /// </summary>
        public static ISimpleStoreAsync DelayRandomly(this ISimpleStoreAsync store, double dtStats, double dtAdd, double dtGet, double dtRemove, double dtTryGetFromCache, double dtFlush)
            => new WrapperRandomDelayAsync(store, dtStats, dtAdd, dtGet, dtRemove, dtTryGetFromCache, dtFlush);
    }
}
