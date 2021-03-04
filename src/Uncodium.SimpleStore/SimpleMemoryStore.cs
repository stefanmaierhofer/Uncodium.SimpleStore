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

#pragma warning disable CS1591

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Uncodium.SimpleStore
{
    public class SimpleMemoryStore : ISimpleStore
    {
        private readonly Dictionary<string, byte[]> m_db;
        private Stats m_stats;

        private bool m_isDisposed = false;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckDisposed() { if (m_isDisposed) throw new ObjectDisposedException(nameof(SimpleMemoryStore)); }

        public SimpleMemoryStore()
        {
            m_db = new Dictionary<string, byte[]>();
        }

        /// <summary>
        /// Various counts and other statistics.
        /// </summary>
        public Stats Stats => m_stats.Copy();

        public string LatestKeyAdded { get; private set; }

        public string LatestKeyFlushed { get; private set; }

        public string Version => Global.Version;

        public void Add(string key, byte[] value)
        {
            CheckDisposed();

            lock (m_db)
            {
                m_db[key] = value;
                LatestKeyAdded = key;
                LatestKeyFlushed = key;
            }
            Interlocked.Increment(ref m_stats.CountAdd);
        }

        public void AddStream(string key, Stream stream, Action<long> onProgress = default, CancellationToken ct = default)
        {
            using var ms = new MemoryStream();

            ct.ThrowIfCancellationRequested();
            if (onProgress != default) onProgress(0L);
            stream.CopyTo(ms);
            var buffer = ms.ToArray();
            if (onProgress != default) onProgress(buffer.Length);

            lock (m_db)
            {
                m_db[key] = buffer;
                LatestKeyAdded =  LatestKeyFlushed = key;
            }
            Interlocked.Increment(ref m_stats.CountAdd);
        }

        public bool Contains(string key)
        {
            CheckDisposed();

            bool result;
            lock (m_db)
            {
                result = m_db.ContainsKey(key);
            }
            Interlocked.Increment(ref m_stats.CountContains);
            return result;
        }

        /// <summary>
        /// Gets size of value in bytes, or null if key does not exist.
        /// </summary>
        public long? GetSize(string key)
        {
            CheckDisposed();

            lock (m_db)
            {
                return m_db.TryGetValue(key, out var buffer) ? buffer.Length : null;
            }
        }

        public byte[] Get(string key)
        {
            CheckDisposed();

            lock (m_db)
            {
                if (m_db.TryGetValue(key, out var buffer))
                {
                    Interlocked.Increment(ref m_stats.CountGet);
                    return buffer;
                }
                else
                {
                    Interlocked.Increment(ref m_stats.CountGetInvalidKey);
                    return null;
                }
            }
        }

        public byte[] GetSlice(string key, long offset, int length)
        {
            CheckDisposed();

            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), "Offset must be greater or equal 0.");
            if (length < 1) throw new ArgumentOutOfRangeException(nameof(offset), "Size must be greater than 0.");

            lock (m_db)
            {
                if (m_db.TryGetValue(key, out var buffer))
                {
                    Interlocked.Increment(ref m_stats.CountGetSlice);
                    if (offset >= buffer.Length) throw new ArgumentOutOfRangeException(nameof(offset), "Offset must be less than length of value buffer.");
                    if (offset + length > buffer.Length) throw new ArgumentOutOfRangeException(nameof(offset), "Offset + size exceeds length of value buffer.");
                    var result = new byte[length];
                    Array.ConstrainedCopy(buffer, (int)offset, result, 0, length);
                    return result;
                }
                else
                {
                    Interlocked.Increment(ref m_stats.CountGetInvalidKey);
                    return null;
                }
            }
        }

        /// <summary>
        /// Get read stream for data with given key.
        /// This is not thread-safe with respect to overwriting or removing existing values.
        /// </summary>
        /// <param name="key">Retrieve data for this key.</param>
        /// <param name="offset">Optional. Start stream at given position.</param>
        public Stream GetStream(string key, long offset = 0L)
        {
            CheckDisposed();

            lock (m_db)
            {
                if (m_db.TryGetValue(key, out var buffer))
                {
                    if (offset < 0L || offset >= buffer.Length) throw new ArgumentOutOfRangeException(
                        nameof(offset), $"Offset {offset:N0} is out of valid range [0, {buffer.Length:N0})."
                        );

                    var stream = new MemoryStream(buffer) { Position = offset };
                    Interlocked.Increment(ref m_stats.CountGetStream);
                    return stream;
                }
                else
                {
                    Interlocked.Increment(ref m_stats.CountGetInvalidKey);
                    return null;
                }
            }
        }

        public void Remove(string key)
        {
            CheckDisposed();

            lock (m_db)
            {
                if (m_db.Remove(key))
                {
                    Interlocked.Increment(ref m_stats.CountRemove);
                }
                else
                {
                    Interlocked.Increment(ref m_stats.CountRemoveInvalidKey);
                }
            }
        }

        public IEnumerable<(string key, long size)> List()
        {
            CheckDisposed();

            lock (m_db)
            {
                Interlocked.Increment(ref m_stats.CountList);
                return m_db.Select(x => (key: x.Key, size: (long)x.Value.Length));
            }
        }

        public void Flush()
        {
            CheckDisposed();
            Interlocked.Increment(ref m_stats.CountFlush);
        }

        public void Dispose()
        {
            CheckDisposed();
            m_isDisposed = true;
        }

        public long GetUsedBytes() => m_db.Values.Select(buffer => buffer.Length).Sum();

        public long GetReservedBytes() => GetUsedBytes();
    }
}
