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
        private Stats m_stats = new Stats();

        /// <summary>
        /// Creates a store in the given folder.
        /// </summary>
        public SimpleFolderStore(string folder)
        {
            Folder = folder;
            if (!Directory.Exists(Folder)) Directory.CreateDirectory(folder);
        }

        /// <summary></summary>
        public Stats Stats => m_stats;

        /// <summary></summary>
        public void Add(string id, object value, Func<byte[]> getEncodedValue)
        {
            Interlocked.Increment(ref m_stats.CountAdd);
            File.WriteAllBytes(GetFileNameFromId(id), getEncodedValue());
        }

        /// <summary></summary>
        public bool Contains(string id)
        {
            Interlocked.Increment(ref m_stats.CountContains);
            return File.Exists(GetFileNameFromId(id));
        }

        /// <summary></summary>
        public void Dispose() { }

        /// <summary></summary>
        public void Flush() { Interlocked.Increment(ref m_stats.CountFlush); }

        /// <summary></summary>
        public byte[] Get(string id)
        {
            Interlocked.Increment(ref m_stats.CountGet);
            try
            {
                var buffer = File.ReadAllBytes(GetFileNameFromId(id));
                Interlocked.Increment(ref m_stats.CountGetCacheMiss);
                return buffer;
            }
            catch
            {
                Interlocked.Increment(ref m_stats.CountGetInvalidKey);
                return null;
            }
        }

        /// <summary></summary>
        public byte[] GetSlice(string id, long offset, int size)
        {
            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), "Offset must be greater or equal 0.");
            if (size < 1) throw new ArgumentOutOfRangeException(nameof(offset), "Size must be greater than 0.");

            Interlocked.Increment(ref m_stats.CountGetSlice);
            try
            {
                using var fs = File.Open(GetFileNameFromId(id), FileMode.Open, FileAccess.Read, FileShare.Read);
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

        /// <summary></summary>
        public Stream OpenReadStream(string id)
        {
            Interlocked.Increment(ref m_stats.CountOpenReadStream);
            try
            {
                return File.Open(GetFileNameFromId(id), FileMode.Open, FileAccess.Read, FileShare.Read);
            }
            catch
            {
                Interlocked.Increment(ref m_stats.CountGetInvalidKey);
                return null;
            }
        }

        /// <summary></summary>
        public void Remove(string id)
        {
            try
            {
                File.Delete(GetFileNameFromId(id));
                Interlocked.Increment(ref m_stats.CountRemove);
            }
            catch
            {
                Interlocked.Increment(ref m_stats.CountRemoveInvalidKey);
            }
        }

        /// <summary></summary>
        public string[] SnapshotKeys()
        {
            Interlocked.Increment(ref m_stats.CountSnapshotKeys);
            return Directory.GetFiles(Folder);
        }

        /// <summary></summary>
        public object TryGetFromCache(string id) => null;
    }
}
