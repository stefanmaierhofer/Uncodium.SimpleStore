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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Uncodium.SimpleStore
{
    public class WrapperAsync : ISimpleStoreAsync
    {
        private readonly ISimpleStore m_store;

        private bool m_isDisposed = false;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckDisposed() { if (m_isDisposed) throw new ObjectDisposedException(nameof(WrapperAsync)); }

        public WrapperAsync(ISimpleStore store)
        {
            m_store = store ?? throw new ArgumentNullException(nameof(store));
        }

        public Task AddAsync(string key, object value, Func<byte[]> getEncodedValue, CancellationToken ct)
            => Wrap(() => { m_store.Add(key, value, getEncodedValue); return 0; }, ct);

        public Task<byte[]> GetAsync(string key, CancellationToken ct)
            => Wrap(() => m_store.Get(key), ct);

        public Task<Stats> GetStatsAsync(CancellationToken ct)
            => Wrap(() => m_store.Stats, ct);

        public Task RemoveAsync(string key, CancellationToken ct)
            => Wrap(() => { m_store.Remove(key); return 0; }, ct);

        public Task<object> TryGetFromCacheAsync(string key, CancellationToken ct)
            => Wrap(() => m_store.TryGetFromCache(key), ct);

        public Task FlushAsync(CancellationToken ct)
            => Wrap(() => { m_store.Flush(); return 0; }, ct);

        public Task<bool> ContainsAsync(string key, CancellationToken ct)
            => Wrap(() => m_store.Contains(key), ct);

        public Task<byte[]> GetSliceAsync(string key, long offset, int length, CancellationToken ct)
            => Wrap(() => m_store.GetSlice(key, offset, length), ct);

        public Task<Stream> OpenReadStreamAsync(string key, CancellationToken ct)
            => Wrap(() => m_store.OpenReadStream(key), ct);

        public Task<string[]> SnapshotKeysAsync(CancellationToken ct)
            => Wrap(() => m_store.SnapshotKeys(), ct);

        public Task<string> GetLatestKeyAddedAsync(CancellationToken ct)
            => Wrap(() => m_store.LatestKeyAdded, ct);

        public Task<string> GetLatestKeyFlushedAsync(CancellationToken ct)
            => Wrap(() => m_store.LatestKeyFlushed, ct);

        public void Dispose()
        {
            CheckDisposed();
            m_store.Dispose();
            m_isDisposed = true;
        }

        private Task<T> Wrap<T>(Func<T> getResult, CancellationToken ct)
        {
            CheckDisposed();

            var tcs = new TaskCompletionSource<T>();
            Task.Run(() =>
            {
                try
                {
                    var result = getResult();
                    if (ct.IsCancellationRequested)
                    {
                        tcs.SetCanceled();
                    }
                    else
                    {
                        tcs.SetResult(result);
                    }
                }
                catch (Exception e)
                {
                    tcs.SetException(e);
                }
            }, ct);
            return tcs.Task;
        }
    }
}
