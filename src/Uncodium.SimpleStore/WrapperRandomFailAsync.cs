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

#pragma warning disable CS1591

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// Each operation may fail with a given probability. 
    /// </summary>
    public class WrapperRandomFailAsync : ISimpleStoreAsync
    {
        private readonly Random m_random = new Random();
        private readonly ISimpleStoreAsync m_store;
        private readonly double m_pStats;
        private readonly double m_pAdd;
        private readonly double m_pGet;
        private readonly double m_pRemove;
        private readonly double m_pTryGetFromCache;
        private readonly double m_pFlush;

        public WrapperRandomFailAsync(ISimpleStoreAsync store,
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

        public WrapperRandomFailAsync(ISimpleStoreAsync store, double pFail) : this(store, pFail, pFail, pFail, pFail, pFail, pFail)
        { }

        public Task<Stats> GetStatsAsync(CancellationToken ct)
            => m_random.NextDouble() < m_pStats ? throw new Exception() : m_store.GetStatsAsync(ct);

        public Task AddAsync(string key, object value, Func<byte[]> getEncodedValue, CancellationToken ct)
            => m_random.NextDouble() < m_pAdd ? throw new Exception() : m_store.AddAsync(key, value, getEncodedValue, ct);

        public Task<byte[]> GetAsync(string key, CancellationToken ct)
            => m_random.NextDouble() < m_pGet ? throw new Exception() : m_store.GetAsync(key, ct);

        public Task RemoveAsync(string key, CancellationToken ct)
            => m_random.NextDouble() < m_pRemove ? throw new Exception() : m_store.RemoveAsync(key, ct);

        public Task<object> TryGetFromCacheAsync(string key, CancellationToken ct)
            => m_random.NextDouble() < m_pTryGetFromCache ? throw new Exception() : m_store.TryGetFromCacheAsync(key, ct);

        public Task FlushAsync(CancellationToken ct)
            => m_random.NextDouble() < m_pFlush ? throw new Exception() : m_store.FlushAsync(ct);

        public void Dispose() => m_store.Dispose();

        public Task<bool> ContainsAsync(string key, CancellationToken ct)
            => m_random.NextDouble() < m_pRemove ? throw new Exception() : m_store.ContainsAsync(key, ct);

        public Task<byte[]> GetSliceAsync(string key, long offset, int length, CancellationToken ct)
            => m_random.NextDouble() < m_pRemove ? throw new Exception() : m_store.GetSliceAsync(key, offset, length, ct);

        public Task<Stream> OpenReadStreamAsync(string key, CancellationToken ct)
            => m_random.NextDouble() < m_pRemove ? throw new Exception() : m_store.OpenReadStreamAsync(key, ct);

        public Task<string[]> SnapshotKeysAsync(CancellationToken ct)
            => m_random.NextDouble() < m_pRemove ? throw new Exception() : m_store.SnapshotKeysAsync(ct);

        public Task<string> GetLatestKeyAddedAsync(CancellationToken ct)
            => m_random.NextDouble() < m_pRemove ? throw new Exception() : m_store.GetLatestKeyAddedAsync( ct);

        public Task<string> GetLatestKeyFlushedAsync(CancellationToken ct)
            => m_random.NextDouble() < m_pRemove ? throw new Exception() : m_store.GetLatestKeyFlushedAsync( ct);
    }
}