﻿/*
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
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// Simple folder store with one file per entry.
    /// </summary>
    public class SimpleFolderStore : ISimpleStore
    {
        /// <summary>
        /// The store folder.
        /// </summary>
        public string Folder { get; }

        private string GetFileNameFromId(string id) => Path.Combine(Folder, id);
        private Stats m_stats;

        private bool m_isDisposed = false;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckDisposed() { if (m_isDisposed) throw new ObjectDisposedException(nameof(SimpleFolderStore)); }

        /// <summary>
        /// Creates a store in the given folder.
        /// </summary>
        public SimpleFolderStore(string folder)
        {
            Folder = folder;
            if (!Directory.Exists(Folder)) Directory.CreateDirectory(folder);
        }

        public Stats Stats => m_stats;

        public string LatestKeyAdded { get; private set; }

        public string LatestKeyFlushed { get; private set; }

        public long GetUsedBytes() 
            => Directory.EnumerateFiles(Folder).Select(s => new FileInfo(s).Length).Sum();

        public long GetReservedBytes() => GetUsedBytes();

        public string Version => Global.Version;

        public void Add(string key, object value, uint flags, Func<byte[]> getEncodedValue)
        {
            CheckDisposed();

            var buffer = getEncodedValue();

            if (flags == (uint)Flags.LZ4)
            {
                var compressed = Utils.EncodeLz4SelfContained(buffer).ToArray();
                File.WriteAllBytes(GetFileNameFromId(key), compressed);
            }
            else
            {
                if (flags != (uint)Flags.None) throw new Exception();
                File.WriteAllBytes(GetFileNameFromId(key), buffer);
            }

            Interlocked.Increment(ref m_stats.CountAdd);
            LatestKeyAdded = key;
            LatestKeyFlushed = key;
        }

        public bool Contains(string key)
        {
            CheckDisposed();
            Interlocked.Increment(ref m_stats.CountContains);
            return File.Exists(GetFileNameFromId(key));
        }

        public void Dispose()
        {
            CheckDisposed();
            m_isDisposed = true;
        }

        public void Flush() 
        {
            CheckDisposed();
            Interlocked.Increment(ref m_stats.CountFlush);
        }

        public byte[] Get(string key)
        {
            CheckDisposed();

            Interlocked.Increment(ref m_stats.CountGet);
            try
            {
                var buffer = File.ReadAllBytes(GetFileNameFromId(key));
                Interlocked.Increment(ref m_stats.CountGetCacheMiss);
                return buffer;
            }
            catch
            {
                Interlocked.Increment(ref m_stats.CountGetInvalidKey);
                return null;
            }
        }

        public byte[] GetSlice(string key, long offset, int size)
        {
            CheckDisposed();

            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), "Offset must be greater or equal 0.");
            if (size < 1) throw new ArgumentOutOfRangeException(nameof(offset), "Size must be greater than 0.");

            Interlocked.Increment(ref m_stats.CountGetSlice);
            try
            {
                using var fs = File.Open(GetFileNameFromId(key), FileMode.Open, FileAccess.Read, FileShare.Read);
                fs.Seek(offset, SeekOrigin.Begin);
                using var br = new BinaryReader(fs);
                var buffer = br.ReadBytes(size);
                Interlocked.Increment(ref m_stats.CountGetCacheMiss);
                return buffer;
            }
            catch
            {
                Interlocked.Increment(ref m_stats.CountGetInvalidKey);
                return null;
            }
        }

        public Stream OpenReadStream(string key)
        {
            CheckDisposed();

            Interlocked.Increment(ref m_stats.CountOpenReadStream);
            try
            {
                return File.Open(GetFileNameFromId(key), FileMode.Open, FileAccess.Read, FileShare.Read);
            }
            catch
            {
                Interlocked.Increment(ref m_stats.CountGetInvalidKey);
                return null;
            }
        }

        public void Remove(string key)
        {
            CheckDisposed();

            try
            {
                File.Delete(GetFileNameFromId(key));
                Interlocked.Increment(ref m_stats.CountRemove);
            }
            catch
            {
                Interlocked.Increment(ref m_stats.CountRemoveInvalidKey);
            }
        }

        public string[] SnapshotKeys()
        {
            CheckDisposed();

            Interlocked.Increment(ref m_stats.CountSnapshotKeys);
            return Directory.GetFiles(Folder);
        }

        public object TryGetFromCache(string key)
        {
            CheckDisposed();
            return null;
        }
    }
}
