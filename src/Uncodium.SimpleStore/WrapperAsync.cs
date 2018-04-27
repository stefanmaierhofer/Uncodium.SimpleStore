using System;
using System.Threading;
using System.Threading.Tasks;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// </summary>
    public class WrapperAsync : ISimpleStoreAsync
    {
        private ISimpleStore m_store;

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
