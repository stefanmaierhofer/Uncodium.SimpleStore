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

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// Each operation may fail with a given probability. 
    /// </summary>
    public class WrapperRandomFail : ISimpleStore
    {
        private readonly Random m_random = new();
        private readonly ISimpleStore m_store;
        private readonly double m_pStats;
        private readonly double m_pAdd;
        private readonly double m_pGet;
        private readonly double m_pRemove;
        private readonly double m_pFlush;

        public WrapperRandomFail(ISimpleStore store,
            double pStats, double pAdd, double pGet, double pRemove, double pFlush
            )
        {
            m_store = store ?? throw new ArgumentNullException(nameof(store));
            m_pStats = pStats;
            m_pAdd = pAdd;
            m_pGet = pGet;
            m_pRemove = pRemove;
            m_pFlush = pFlush;
        }

        public WrapperRandomFail(ISimpleStore store, double pFail) : this(store, pFail, pFail, pFail, pFail, pFail)
        { }

        public Stats Stats
            => m_random.NextDouble() < m_pStats ? throw new Exception() : m_store.Stats;

        public string Version => m_store.Version;

        public void Add(string key, byte[] value)
        {
            if (m_random.NextDouble() < m_pAdd) throw new Exception();
            m_store.Add(key, value);
        }

        public void AddStream(string key, Stream stream)
        {
            if (m_random.NextDouble() < m_pAdd) throw new Exception();
            m_store.AddStream(key, stream);
        }

        public bool Contains(string key)
            => m_random.NextDouble() < m_pGet ? throw new Exception() : m_store.Contains(key);
        
        public long? GetSize(string key)
            => m_random.NextDouble() < m_pGet ? throw new Exception() : m_store.GetSize(key);

        public byte[] Get(string key)
            => m_random.NextDouble() < m_pGet ? throw new Exception() : m_store.Get(key);

        public byte[] GetSlice(string key, long offset, int length)
            => m_random.NextDouble() < m_pGet ? throw new Exception() : m_store.GetSlice(key, offset, length);

        public Stream GetStream(string key, long offset = 0L)
            => m_random.NextDouble() < m_pGet ? throw new Exception() : m_store.GetStream(key, offset);

        public void Remove(string key)
        {
            if (m_random.NextDouble() < m_pRemove) throw new Exception();
            m_store.Remove(key);
        }

        public IEnumerable<(string key, long size)> List() => m_store.List();

        public void Flush()
        {
            if (m_random.NextDouble() < m_pFlush) throw new Exception();
            m_store.Flush();
        }

        public void Dispose() => m_store.Dispose();

        public long GetUsedBytes()
            => m_random.NextDouble() < m_pGet ? throw new Exception() : m_store.GetUsedBytes();

        public long GetReservedBytes()
            => m_random.NextDouble() < m_pGet ? throw new Exception() : m_store.GetReservedBytes();
    }
}
