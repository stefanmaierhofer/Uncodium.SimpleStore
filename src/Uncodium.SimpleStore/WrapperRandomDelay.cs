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
using System.IO;
using System.Threading;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// Each operation will be delayed. 
    /// </summary>
    public class WrapperRandomDelay : ISimpleStore
    {
        private readonly Random m_random = new();
        private readonly ISimpleStore m_store;
        private readonly double m_dtStats;
        private readonly double m_dtAdd;
        private readonly double m_dtContains;
        private readonly double m_dtGet;
        private readonly double m_dtRemove;
        private readonly double m_dtTryGetFromCache;
        private readonly double m_dtFlush;

        public WrapperRandomDelay(ISimpleStore store,
            double dtStats, double dtAdd, double dtContains, double dtGet, double dtRemove, double dtTryGetFromCache, double dtFlush
            )
        {
            m_store = store ?? throw new ArgumentNullException(nameof(store));
            m_dtStats = dtStats;
            m_dtAdd = dtAdd;
            m_dtContains = dtContains;
            m_dtGet = dtGet;
            m_dtRemove = dtRemove;
            m_dtTryGetFromCache = dtTryGetFromCache;
            m_dtFlush = dtFlush;
        }

        public WrapperRandomDelay(ISimpleStore store, double dt) : this(store, dt, dt, dt, dt, dt, dt, dt)
        { }

        public Stats Stats
        {
            get
            {
                Thread.Sleep(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtStats));
                return m_store.Stats;
            }
        }

        public string LatestKeyAdded => m_store.LatestKeyAdded;

        public string LatestKeyFlushed => m_store.LatestKeyFlushed;

        public string Version => m_store.Version;

        public void Add(string key, object value, uint flags, Func<byte[]> getEncodedValue)
        {
            Thread.Sleep(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtAdd));
            m_store.Add(key, value, flags, getEncodedValue);
        }

        public bool Contains(string key)
        {
            Thread.Sleep(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtContains));
            return m_store.Contains(key);
        }

        public byte[] Get(string key)
        {
            Thread.Sleep(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtGet));
            return m_store.Get(key);
        }

        public byte[] GetSlice(string key, long offset, int length)
        {
            Thread.Sleep(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtGet));
            return m_store.GetSlice(key, offset, length);
        }

        public Stream OpenReadStream(string key)
        {
            Thread.Sleep(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtGet));
            return m_store.OpenReadStream(key);
        }

        public void Remove(string key)
        {
            Thread.Sleep(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtRemove));
            m_store.Remove(key);
        }

        public object TryGetFromCache(string key)
        {
            Thread.Sleep(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtTryGetFromCache));
            return m_store.TryGetFromCache(key);
        }

        public string[] SnapshotKeys() => m_store.SnapshotKeys();

        public void Flush()
        {
            Thread.Sleep(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtFlush));
            m_store.Flush();
        }

        public void Dispose() => m_store.Dispose();

        public long GetUsedBytes()
        {
            Thread.Sleep(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtFlush));
            return m_store.GetUsedBytes();
        }

        public long GetReservedBytes()
        {
            Thread.Sleep(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtFlush));
            return m_store.GetReservedBytes();
        }
    }
}
