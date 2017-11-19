namespace Uncodium.SimpleStore
{
    /// <summary>
    /// </summary>
    public struct Stats
    {
        /// <summary>
        /// </summary>
        public long CountAdd;

        /// <summary>
        /// </summary>
        public long CountGet;

        /// <summary>
        /// </summary>
        public long CountGetInvalidKey;

        /// <summary>
        /// </summary>
        public long CountGetCacheHit;

        /// <summary>
        /// </summary>
        public long CountGetCacheMiss;

        /// <summary>
        /// </summary>
        public long CountRemove;

        /// <summary>
        /// </summary>
        public long CountRemoveInvalidKey;

        /// <summary>
        /// </summary>
        public long CountKeepAlive;

        /// <summary>
        /// </summary>
        public long CountSnapshotKeys;

        /// <summary>
        /// </summary>
        public long CountFlush;
    }
}
