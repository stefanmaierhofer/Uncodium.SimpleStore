/*
   MIT License
   
   Copyright (c) 2014,2015,2016,2017,2018,2019,2020,2021,2022 Stefan Maierhofer.
   
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
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Uncodium.SimpleStore;

/// <summary>
/// </summary>
public interface ISimpleStoreAsync : IDisposable
{
    /// <summary>
    /// Add data from buffer.
    /// </summary>
    Task AddAsync(string key, byte[] data, Action<long>? onProgress = default, CancellationToken ct = default);

    /// <summary>
    /// Add data from stream.
    /// </summary>
    Task AddStreamAsync(string key, Stream data, Action<long>? onProgress = default, CancellationToken ct = default);

    /// <summary>
    /// True if key exists in store.
    /// </summary>
    Task<bool> ContainsAsync(string key, CancellationToken ct = default);

    /// <summary>
    /// Get size of value in bytes,
    /// or null if key does not exist.
    /// </summary>
    Task<long?> GetSizeAsync(string key, CancellationToken ct = default);

    /// <summary>
    /// Get value as buffer,
    /// or null if key does not exist.
    /// </summary>
    Task<byte[]?> GetAsync(string key, CancellationToken ct = default);

    /// <summary>
    /// Get value slice as buffer,
    /// or null if key does not exist.
    /// </summary>
    Task<byte[]?> GetSliceAsync(string key, long offset, int length, CancellationToken ct = default);

    /// <summary>
    /// Get value as read stream,
    /// or null if key does not exist.
    /// This is not thread-safe with respect to overwriting or removing existing values.
    /// </summary>
    /// <param name="key">Retrieve data for this key.</param>
    /// <param name="offset">Optional. Start stream at given position.</param>
    /// <param name="ct"></param>
    Task<Stream?> GetStreamAsync(string key, long offset = 0L, CancellationToken ct = default);

    /// <summary>
    /// Enumerate all entries as (key, size) tuples.
    /// </summary>
    IAsyncEnumerable<(string key, long size)> ListAsync(CancellationToken ct = default);

    /// <summary>
    /// Remove entry.
    /// </summary>
    Task RemoveAsync(string key, CancellationToken ct = default);

    /// <summary>
    /// Commit any pending changes to backing storage.
    /// </summary>
    Task FlushAsync(CancellationToken ct = default);

    /// <summary>
    /// Total bytes used for data.
    /// </summary>
    Task<long> GetUsedBytesAsync(Action<long>? onProgress = default, CancellationToken ct = default);

    /// <summary>
    /// Total bytes reserved for data.
    /// </summary>
    Task<long> GetReservedBytesAsync(Action<long>? onProgress = default, CancellationToken ct = default);

    /// <summary>
    /// Current version.
    /// </summary>
    string Version { get; }

    /// <summary>
    /// Various runtime counts and other statistics.
    /// </summary>
    Stats Stats { get; }

    /// <summary>
    /// </summary>
    public bool IsDisposed { get; }
}

