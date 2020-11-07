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

#pragma warning disable CS1591

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// Each operation will be delayed. 
    /// </summary>
    public class WrapperRandomDelayAsync : ISimpleStoreAsync
    {
        private readonly Random m_random = new Random();
        private readonly ISimpleStoreAsync m_store;
        private readonly double m_dtStats;
        private readonly double m_dtAdd;
        private readonly double m_dtGet;
        private readonly double m_dtRemove;
        private readonly double m_dtTryGetFromCache;
        private readonly double m_dtFlush;

        public WrapperRandomDelayAsync(ISimpleStoreAsync store,
            double dtStats, double dtAdd, double dtGet, double dtRemove, double dtTryGetFromCache, double dtFlush
            )
        {
            m_store = store ?? throw new ArgumentNullException(nameof(store));
            m_dtStats = dtStats;
            m_dtAdd = dtAdd;
            m_dtGet = dtGet;
            m_dtRemove = dtRemove;
            m_dtTryGetFromCache = dtTryGetFromCache;
            m_dtFlush = dtFlush;
        }

        public WrapperRandomDelayAsync(ISimpleStoreAsync store, double dt) : this(store, dt, dt, dt, dt, dt, dt)
        { }

        public async Task<Stats> GetStatsAsync(CancellationToken ct)
        {
            await Task.Delay(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtStats), ct);
            return await m_store.GetStatsAsync(ct);
        }

        public async Task AddAsync(string key, object value, Func<byte[]> getEncodedValue, CancellationToken ct)
        {
            await Task.Delay(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtAdd), ct);
            await m_store.AddAsync(key, value, getEncodedValue, ct);
        }

        public async Task<byte[]> GetAsync(string key, CancellationToken ct)
        {
            await Task.Delay(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtGet), ct);
            return await m_store.GetAsync(key, ct);
        }

        public async Task RemoveAsync(string key, CancellationToken ct)
        {
            await Task.Delay(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtRemove), ct);
            await m_store.RemoveAsync(key, ct);
        }

        public async Task<object> TryGetFromCacheAsync(string key, CancellationToken ct)
        {
            await Task.Delay(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtTryGetFromCache), ct);
            return await m_store.TryGetFromCacheAsync(key, ct);
        }

        public async Task FlushAsync(CancellationToken ct)
        {
            await Task.Delay(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtFlush), ct);
            await m_store.FlushAsync(ct);
        }

        public void Dispose() => m_store.Dispose();

        public async Task<bool> ContainsAsync(string key, CancellationToken ct)
        {
            await Task.Delay(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtFlush), ct);
            return await m_store.ContainsAsync(key, ct);
        }

        public async Task<byte[]> GetSliceAsync(string key, long offset, int length, CancellationToken ct)
        {
            await Task.Delay(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtFlush), ct);
            return await m_store.GetSliceAsync(key, offset, length, ct);
        }

        public async Task<Stream> OpenReadStreamAsync(string key, CancellationToken ct)
        {
            await Task.Delay(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtFlush), ct);
            return await m_store.OpenReadStreamAsync(key, ct);
        }

        public async Task<string[]> SnapshotKeysAsync(CancellationToken ct)
        {
            await Task.Delay(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtFlush), ct);
            return await m_store.SnapshotKeysAsync(ct);
        }

        public async Task<string> GetLatestKeyAddedAsync(CancellationToken ct)
        {
            await Task.Delay(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtFlush), ct);
            return await m_store.GetLatestKeyAddedAsync( ct);
        }

        public async Task<string> GetLatestKeyFlushedAsync(CancellationToken ct)
        {
            await Task.Delay(TimeSpan.FromSeconds(m_random.NextDouble() * m_dtFlush), ct);
            return await m_store.GetLatestKeyFlushedAsync( ct);
        }
    }
}
