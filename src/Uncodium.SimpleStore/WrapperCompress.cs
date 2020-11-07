﻿/*
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
using System.IO.Compression;
using System.Text;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// Compressed storage. 
    /// </summary>
    public class WrapperCompress : ISimpleStore
    {
        private readonly ISimpleStore m_store;
        private readonly CompressionLevel m_compressionLevel;

        /// <summary>
        /// </summary>
        public WrapperCompress(ISimpleStore store, CompressionLevel compressionLevel)
        {
            m_store = store ?? throw new ArgumentNullException(nameof(store));
            m_compressionLevel = compressionLevel;
        }

        /// <summary>
        /// </summary>
        public Stats Stats => m_store.Stats;

        /// <summary>
        /// </summary>
        public void Add(string key, object value, Func<byte[]> getEncodedValue)
        {
            if (getEncodedValue == null)
            {
                m_store.Add(key, value, getEncodedValue);
                return;
            }

            m_store.Add(key, value, f);

            byte[] f()
            {
                var ms = new MemoryStream();
                var data = getEncodedValue();
                using (var bw = new BinaryWriter(ms))
                {
                    bw.Write(data.Length);
                    using var zs = new GZipStream(ms, m_compressionLevel, false);
                    zs.Write(data, 0, data.Length);
                }
                return ms.ToArray();
            }
        }

        /// <summary>
        /// </summary>
        public bool Contains(string key) => m_store.Contains(key);

        /// <summary>
        /// </summary>
        public byte[] Get(string key)
        {
            var buffer = m_store.Get(key);
            if (buffer == null) return null;
            var ms = new MemoryStream(buffer);
            using var br = new BinaryReader(ms);
            var count = br.ReadInt32();
            var data = new byte[count];
            using var zs = new GZipStream(ms, CompressionMode.Decompress);
            var l = zs.Read(data, 0, count);
            if (l != count) throw new Exception($"Read {l} bytes instead of {count} bytes.");
            return data;
        }

        /// <summary>
        /// </summary>
        public byte[] GetSlice(string key, long offset, int length)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// </summary>
        public Stream OpenReadStream(string key)
        {
            var buffer = m_store.Get(key);
            if (buffer == null) return null;
            var ms = new MemoryStream(buffer, 4, buffer.Length - 4);
            var zs = new GZipStream(ms, CompressionMode.Decompress);
            return zs;
        }

        /// <summary>
        /// </summary>
        public void Remove(string key)
        {
            m_store.Remove(key);
        }

        /// <summary>
        /// </summary>
        public object TryGetFromCache(string key)
        {
            return m_store.TryGetFromCache(key);
        }

        /// <summary>
        /// </summary>
        public string[] SnapshotKeys() => m_store.SnapshotKeys();

        /// <summary>
        /// </summary>
        public void Flush()
        {
            m_store.Flush();
        }

        /// <summary>
        /// </summary>
        public void Dispose() => m_store.Dispose();
    }
}
