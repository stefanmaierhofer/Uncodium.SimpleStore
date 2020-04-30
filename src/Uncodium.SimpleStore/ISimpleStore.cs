using System;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// 
    /// </summary>
    public interface ISimpleStore : IDisposable
    {
        /// <summary>
        /// Various runtime counts and other statistics.
        /// </summary>
        Stats Stats { get; }

        /// <summary>
        /// Adds key/value 
        /// </summary>
        void Add(string key, object value, Func<byte[]> getEncodedValue);

        /// <summary>
        /// True if key is contained in store.
        /// </summary>
        bool Contains(string key);

        /// <summary>
        /// Get value from key.
        /// </summary>
        byte[] Get(string key);

        /// <summary>
        /// Remove entry.
        /// </summary>
        void Remove(string key);

        /// <summary>
        /// </summary>
        object TryGetFromCache(string key);

        /// <summary>
        /// Gets a snapshot of all existing keys.
        /// </summary>
        string[] SnapshotKeys();

        /// <summary>
        /// Commit pending changes to storage.
        /// </summary>
        void Flush();
    }
}
