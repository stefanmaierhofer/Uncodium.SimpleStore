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
using System.Threading;
using System.Threading.Tasks;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// </summary>
    public class WrapperAsync : ISimpleStoreAsync
    {
        private readonly ISimpleStore m_store;

        /// <summary>
        /// </summary>
        public WrapperAsync(ISimpleStore store)
        {
            m_store = store ?? throw new ArgumentNullException(nameof(store));
        }

        /// <summary>
        /// </summary>
        public Task AddAsync(string key, object value, Func<byte[]> getEncodedValue, CancellationToken ct)
        {
            var tcs = new TaskCompletionSource<int>();
            Task.Run(() =>
            {
                try
                {
                    m_store.Add(key, value, getEncodedValue);
                    if (ct.IsCancellationRequested)
                    {
                        tcs.SetCanceled();
                    }
                    else
                    {
                        tcs.SetResult(0);
                    }
                }
                catch (Exception e)
                {
                    tcs.SetException(e);
                }
            }, ct);
            return tcs.Task;
        }

        /// <summary>
        /// </summary>
        public Task<byte[]> GetAsync(string key, CancellationToken ct)
        {
            var tcs = new TaskCompletionSource<byte[]>();
            Task.Run(() =>
            {
                try
                {
                    var result = m_store.Get(key);
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

        /// <summary>
        /// </summary>
        public Task<Stats> GetStatsAsync(CancellationToken ct)
        {
            var tcs = new TaskCompletionSource<Stats>();
            Task.Run(() =>
            {
                try
                {
                    var result = m_store.Stats;
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

        /// <summary>
        /// </summary>
        public Task RemoveAsync(string key, CancellationToken ct)
        {
            var tcs = new TaskCompletionSource<int>();
            Task.Run(() =>
            {
                try
                {
                    m_store.Remove(key);
                    if (ct.IsCancellationRequested)
                    {
                        tcs.SetCanceled();
                    }
                    else
                    {
                        tcs.SetResult(0);
                    }
                }
                catch (Exception e)
                {
                    tcs.SetException(e);
                }
            }, ct);
            return tcs.Task;
        }

        /// <summary>
        /// </summary>
        public Task<object> TryGetFromCacheAsync(string key, CancellationToken ct)
        {
            var tcs = new TaskCompletionSource<object>();
            Task.Run(() =>
            {
                try
                {
                    var result = m_store.TryGetFromCache(key);
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

        /// <summary>
        /// </summary>
        public Task FlushAsync(CancellationToken ct)
        {
            var tcs = new TaskCompletionSource<int>();
            Task.Run(() =>
            {
                try
                {
                    m_store.Flush();
                    if (ct.IsCancellationRequested)
                    {
                        tcs.SetCanceled();
                    }
                    else
                    {
                        tcs.SetResult(0);
                    }
                }
                catch (Exception e)
                {
                    tcs.SetException(e);
                }
            }, ct);
            return tcs.Task;
        }

        /// <summary>
        /// </summary>
        public void Dispose()
        {
            m_store.Dispose();
        }
    }
}
