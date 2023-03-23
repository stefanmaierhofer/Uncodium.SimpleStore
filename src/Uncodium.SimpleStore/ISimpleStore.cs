/*
   MIT License
   
   Copyright (c) 2014,2015,2016,2017,2018,2019,2020,2021,2022,2023 Stefan Maierhofer.
   
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

namespace Uncodium.SimpleStore;

/// <summary>
/// </summary>
public interface ISimpleStore : IDisposable
{
    /// <summary>
    /// Add data from buffer.
    /// </summary>
    void Add(string key, byte[] data);

    /// <summary>
    /// Add data from stream.
    /// </summary>
    void AddStream(string key, Stream data, Action<long>? onProgress = default, CancellationToken ct = default);

    /// <summary>
    /// Get write stream for given key.
    /// </summary>
    Stream GetWriteStream(string key, bool overwrite = true, Action<long>? onProgress = default, CancellationToken ct = default);

    /// <summary>
    /// True if key exists in store.
    /// </summary>
    bool Contains(string key);

    /// <summary>
    /// Get size of value in bytes,
    /// or null if key does not exist.
    /// </summary>
    long? GetSize(string key);

    /// <summary>
    /// Get value as buffer,
    /// or null if key does not exist.
    /// </summary>
    byte[]? Get(string key);

    /// <summary>
    /// Get value slice as buffer,
    /// or null if key does not exist.
    /// </summary>
    byte[]? GetSlice(string key, long offset, int length);

    /// <summary>
    /// Get value as read stream,
    /// or null if key does not exist.
    /// This is not thread-safe with respect to overwriting or removing existing values.
    /// </summary>
    /// <param name="key">Retrieve data for this key.</param>
    /// <param name="offset">Optional. Start stream at given position.</param>
    Stream? GetStream(string key, long offset = 0L);

    /// <summary>
    /// Enumerate all entries as (key, size) tuples.
    /// </summary>
    IEnumerable<(string key, long size)> List();

    /// <summary>
    /// Remove entry.
    /// </summary>
    void Remove(string key);

    /// <summary>
    /// Commit any pending changes to backing storage.
    /// </summary>
    void Flush();

    /// <summary>
    /// Total bytes used for data.
    /// </summary>
    long GetUsedBytes();

    /// <summary>
    /// Total bytes reserved for data.
    /// </summary>
    long GetReservedBytes();

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
    bool IsDisposed { get; }
}
