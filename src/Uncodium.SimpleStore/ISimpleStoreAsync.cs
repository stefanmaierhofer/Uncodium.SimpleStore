/*
   MIT License
   
   Copyright (c) 2014,2015,2016,2017,2018,2019,2020 Stefan Maierhofer.
   
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
        /// Adds key/value 
        /// </summary>
        Task AddAsync(string key, object value, Func<byte[]> getEncodedValue, CancellationToken ct);

        /// <summary>
        /// True if key is contained in store.
        /// </summary>
        Task<bool> ContainsAsync(string key, CancellationToken ct);

        /// <summary>
        /// </summary>
        Task<byte[]> GetAsync(string key, CancellationToken ct);

        /// <summary>
        /// Get slice of value from key,
        /// or null if key does not exist.
        /// </summary>
        Task<byte[]> GetSliceAsync(string key, long offset, int length, CancellationToken ct);

        /// <summary>
        /// Get read stream for value from key.
        /// This is not thread-safe with respect to overwriting or removing existing values.
        /// </summary>
        Task<Stream> OpenReadStreamAsync(string key, CancellationToken ct);

        /// <summary>
        /// </summary>
        Task RemoveAsync(string key, CancellationToken ct);

        /// <summary>
        /// </summary>
        Task<object> TryGetFromCacheAsync(string key, CancellationToken ct);
        
        /// <summary>
        /// Gets a snapshot of all existing keys.
        /// </summary>
        Task<string[]> SnapshotKeysAsync(CancellationToken ct);

        /// <summary>
        /// </summary>
        Task FlushAsync(CancellationToken ct);

        /// <summary>
        /// Gets latest key added to the store.
        /// </summary>
        Task<string> GetLatestKeyAddedAsync(CancellationToken ct);

        /// <summary>
        /// Gets latest key flushed to backing storage.
        /// </summary>
        Task<string> GetLatestKeyFlushedAsync(CancellationToken ct);
    }
}
