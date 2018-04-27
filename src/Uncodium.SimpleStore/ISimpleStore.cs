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
        /// </summary>
        byte[] Get(string key);

        /// <summary>
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
        /// </summary>
        void Flush();
    }
}
