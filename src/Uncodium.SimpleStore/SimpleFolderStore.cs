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

        public Stats Stats => m_stats.Copy();

        public string LatestKeyAdded { get; private set; }

        public string LatestKeyFlushed { get; private set; }

        public long GetUsedBytes()
            => Directory.EnumerateFiles(Folder).Select(s => new FileInfo(s).Length).Sum();

        public long GetReservedBytes() => GetUsedBytes();

        public string Version => Global.Version;

        public void Add(string key, byte[] value)
        {
            CheckDisposed();

            File.WriteAllBytes(GetFileNameFromId(key), value);

            Interlocked.Increment(ref m_stats.CountAdd);
            LatestKeyAdded = LatestKeyFlushed = key;
        }

        public void AddStream(string key, Stream stream)
        {
            var filename = GetFileNameFromId(key);

            using var target = File.Open(filename, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);
            target.Position = 0L;

            stream.CopyTo(target);

            Interlocked.Increment(ref m_stats.CountAdd);
            LatestKeyAdded = LatestKeyFlushed = key;
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
                Interlocked.Increment(ref m_stats.CountGet);
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
                Interlocked.Increment(ref m_stats.CountGet);
                return buffer;
            }
            catch
            {
                Interlocked.Increment(ref m_stats.CountGetInvalidKey);
                return null;
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

            Interlocked.Increment(ref m_stats.CountGetStream);
            try
            {
                var stream = File.Open(GetFileNameFromId(key), FileMode.Open, FileAccess.Read, FileShare.Read);
                stream.Position = offset;
                Interlocked.Increment(ref m_stats.CountGetStream);
                return stream;
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

        public IEnumerable<(string key, long size)> List()
        {
            CheckDisposed();
            var skip = Folder.Length + 1;
            return Directory
                .EnumerateFiles(Folder, "*.*", SearchOption.AllDirectories)
                .Select(x => (key: x.Substring(skip), size: new FileInfo(x).Length))
                ;
        }
    }
}
