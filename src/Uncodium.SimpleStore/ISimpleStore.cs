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
        /// </summary>
        void Add(string id, object value, Func<byte[]> getEncodedValue);

        /// <summary>
        /// </summary>
        byte[] Get(string id);

        /// <summary>
        /// </summary>
        void Remove(string id);

        /// <summary>
        /// </summary>
        object TryGetFromCache(string id);

        /// <summary>
        /// Gets a snapshot of all existing keys.
        /// </summary>
        string[] SnapshotKeys();

        /// <summary>
        /// </summary>
        void Flush();
    }
}
