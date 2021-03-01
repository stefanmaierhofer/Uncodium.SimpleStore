/*
   MIT License
   
   Copyright (c) 2014,2015,2016,2017,2018,2019,2020,2021 Stefan Maierhofer.
   
   Permission is hereby granted, free of charge, to any person obtaining a copy
   of this software and associated documentation files (the "Software"), to deal
   in the Software without restriction, including without limitation the rights
   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
   copies of the Software, and to permit persons to whom the Software is
   furnished to do so, subject to the following conditions:
   
   The above copyright notice and this permission notice shall be included in all
   copies or substantial portions of the Software.
   
   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
*/

using System;
using System.IO;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// </summary>
    public interface ISimpleStore : IDisposable
    {
        /// <summary>
        /// Various runtime counts and other statistics.
        /// </summary>
        Stats Stats { get; }

        /// <summary>
        /// Adds key/value .
        /// </summary>
        void Add(string key, object value, Func<byte[]> getEncodedValue);

        /// <summary>
        /// Adds key/value, where value is the content of given stream.
        /// </summary>
        void Add(string key, Stream stream);

        /// <summary>
        /// True if key is contained in store.
        /// </summary>
        bool Contains(string key);

        /// <summary>
        /// Get value from key,
        /// or null if key does not exist.
        /// </summary>
        byte[] Get(string key);

        /// <summary>
        /// Get slice of value from key,
        /// or null if key does not exist.
        /// </summary>
        byte[] GetSlice(string key, long offset, int length);


        /// <summary>
        /// Get read stream for data with given key.
        /// This is not thread-safe with respect to overwriting or removing existing values.
        /// </summary>
        /// <param name="key">Retrieve data for this key.</param>
        /// <param name="offset">Optional. Start stream at given position.</param>
        Stream OpenReadStream(string key, long offset = 0L);

        /// <summary>
        /// Remove entry.
        /// </summary>
        void Remove(string key);

        /// <summary>
        /// Returns decoded value from cache, or null if not available.
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

        /// <summary>
        /// Gets latest key added to the store.
        /// </summary>
        string LatestKeyAdded { get; }

        /// <summary>
        /// Gets latest key flushed to backing storage.
        /// In SimpleDiskStore and SimpleFolderStore this is a file on disk.
        /// In SimpleMemoryStore this is memory, so LatestKeyFlushed is always identical to LatestKeyAdded.
        /// </summary>
        string LatestKeyFlushed { get; }

        /// <summary>
        /// Total bytes used for blob storage.
        /// </summary>
        long GetUsedBytes();

        /// <summary>
        /// Total bytes reserved for blob storage.
        /// </summary>
        long GetReservedBytes();

        /// <summary>
        /// Current version.
        /// </summary>
        string Version { get; }
    }
}
