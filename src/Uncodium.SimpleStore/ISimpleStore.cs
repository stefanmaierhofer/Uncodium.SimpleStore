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
using System.Collections.Generic;
using System.IO;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// </summary>
    public interface ISimpleStore : IDisposable
    {
        /// <summary>
        /// Add data for key.
        /// </summary>
        void Add(string key, byte[] data);

        /// <summary>
        /// Add key/value.
        /// </summary>
        void AddStream(string key, Stream data);

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
        Stream GetStream(string key, long offset = 0L);

        /// <summary>
        /// Enumerate all entries.
        /// </summary>
        IEnumerable<(string key, long size)> List();

        /// <summary>
        /// Remove entry.
        /// </summary>
        void Remove(string key);

        /// <summary>
        /// Commit pending changes to backing storage.
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
        /// Get current version.
        /// </summary>
        string Version { get; }

        /// <summary>
        /// Get various runtime counts and other statistics.
        /// </summary>
        Stats Stats { get; }
    }
}
