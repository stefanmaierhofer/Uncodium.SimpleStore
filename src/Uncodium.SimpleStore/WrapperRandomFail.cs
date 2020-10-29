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

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// Each operation may fail with a given probability. 
    /// </summary>
    public class WrapperRandomFail : ISimpleStore
    {
        private readonly Random m_random = new Random();
        private readonly ISimpleStore m_store;
        private readonly double m_pStats;
        private readonly double m_pAdd;
        private readonly double m_pGet;
        private readonly double m_pRemove;
        private readonly double m_pTryGetFromCache;
        private readonly double m_pFlush;

        /// <summary>
        /// </summary>
        public WrapperRandomFail(ISimpleStore store,
            double pStats, double pAdd, double pGet, double pRemove, double pTryGetFromCache, double pFlush
            )
        {
            m_store = store ?? throw new ArgumentNullException(nameof(store));
            m_pStats = pStats;
            m_pAdd = pAdd;
            m_pGet = pGet;
            m_pRemove = pRemove;
            m_pTryGetFromCache = pTryGetFromCache;
            m_pFlush = pFlush;
        }

        /// <summary>
        /// </summary>
        public WrapperRandomFail(ISimpleStore store, double pFail) : this(store, pFail, pFail, pFail, pFail, pFail, pFail)
        { }

        /// <summary>
        /// </summary>
        public Stats Stats
            => m_random.NextDouble() < m_pStats ? throw new Exception() : m_store.Stats;

        /// <summary>
        /// </summary>
        public void Add(string key, object value, Func<byte[]> getEncodedValue)
        {
            if (m_random.NextDouble() < m_pAdd) throw new Exception();
            m_store.Add(key, value, getEncodedValue);
        }

        /// <summary>
        /// </summary>
        public bool Contains(string key)
            => m_random.NextDouble() < m_pGet ? throw new Exception() : m_store.Contains(key);

        /// <summary>
        /// </summary>
        public byte[] Get(string key)
            => m_random.NextDouble() < m_pGet ? throw new Exception() : m_store.Get(key);

        /// <summary>
        /// </summary>
        public byte[] GetSlice(string key, long offset, int length)
            => m_random.NextDouble() < m_pGet ? throw new Exception() : m_store.GetSlice(key, offset, length);

        /// <summary>
        /// </summary>
        public Stream OpenReadStream(string key)
            => m_random.NextDouble() < m_pGet ? throw new Exception() : m_store.OpenReadStream(key);

        /// <summary>
        /// </summary>
        public void Remove(string key)
        {
            if (m_random.NextDouble() < m_pRemove) throw new Exception();
            m_store.Remove(key);
        }

        /// <summary>
        /// </summary>
        public object TryGetFromCache(string key)
            => m_random.NextDouble() < m_pTryGetFromCache ? throw new Exception() : m_store.TryGetFromCache(key);

        /// <summary>
        /// </summary>
        public string[] SnapshotKeys() => m_store.SnapshotKeys();

        /// <summary>
        /// </summary>
        public void Flush()
        {
            if (m_random.NextDouble() < m_pFlush) throw new Exception();
            m_store.Flush();
        }

        /// <summary>
        /// </summary>
        public void Dispose() => m_store.Dispose();
    }
}
