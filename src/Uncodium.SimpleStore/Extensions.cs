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
using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// Store extensions. 
    /// </summary>
    public static class Extensions
    {
        /// <summary>
        /// Get MD5 hash of string as Guid.
        /// </summary>
        public static Guid ToMd5Hash(this string s)
            => new(MD5.Create().ComputeHash(Encoding.UTF8.GetBytes(s)));

        /// <summary>
        /// String will be stored UTF8 encoded.
        /// </summary>
        public static void Add(this ISimpleStore store, string key, string value)
            => store.Add(key, value, () => Encoding.UTF8.GetBytes(value));

        /// <summary>
        /// Store blob.
        /// </summary>
        public static void Add(this ISimpleStore store, string key, byte[] value)
            => store.Add(key, value, () => value);

        /// <summary>
        /// Compressed storage.
        /// </summary>
        public static ISimpleStore Compress(this ISimpleStore store, CompressionLevel compressionLevel)
            => new WrapperCompress(store, compressionLevel);

        /// <summary>
        /// Each store operation fails with given probability.
        /// </summary>
        public static ISimpleStore FailRandomly(this ISimpleStore store, double pFail)
            => new WrapperRandomFail(store, pFail);

        /// <summary>
        /// Each store operation fails with given probability.
        /// </summary>
        public static ISimpleStore FailRandomly(this ISimpleStore store, double pStats, double pAdd, double pGet, double pRemove, double pTryGetFromCache, double pFlush)
            => new WrapperRandomFail(store, pStats, pAdd, pGet, pRemove, pTryGetFromCache, pFlush);

        /// <summary>
        /// Each store operation is delayed between 0 and given duration in seconds.
        /// </summary>
        public static ISimpleStore DelayRandomly(this ISimpleStore store, double dt)
            => new WrapperRandomDelay(store, dt);

        /// <summary>
        /// Each store operation is delayed between 0 and given duration in seconds.
        /// </summary>
        public static ISimpleStore DelayRandomly(this ISimpleStore store, double dtStats, double dtAdd, double dtContains, double dtGet, double dtRemove, double dtTryGetFromCache, double dtFlush)
            => new WrapperRandomDelay(store, dtStats, dtAdd, dtContains, dtGet, dtRemove, dtTryGetFromCache, dtFlush);

        /// <summary>
        /// Makes all store operations async.
        /// </summary>
        public static ISimpleStoreAsync Async(this ISimpleStore store)
            => new WrapperAsync(store);

        /// <summary>
        /// Each store operation fails with given probability.
        /// </summary>
        public static ISimpleStoreAsync FailRandomly(this ISimpleStoreAsync store, double pFail)
            => new WrapperRandomFailAsync(store, pFail);

        /// <summary>
        /// Each store operation fails with given probability.
        /// </summary>
        public static ISimpleStoreAsync FailRandomly(this ISimpleStoreAsync store, double pStats, double pAdd, double pGet, double pRemove, double pTryGetFromCache, double pFlush)
            => new WrapperRandomFailAsync(store, pStats, pAdd, pGet, pRemove, pTryGetFromCache, pFlush);

        /// <summary>
        /// Each store operation is delayed between 0 and given duration in seconds.
        /// </summary>
        public static ISimpleStoreAsync DelayRandomly(this ISimpleStoreAsync store, double dt)
            => new WrapperRandomDelayAsync(store, dt);

        /// <summary>
        /// Each store operation is delayed between 0 and given duration in seconds.
        /// </summary>
        public static ISimpleStoreAsync DelayRandomly(this ISimpleStoreAsync store, double dtStats, double dtAdd, double dtGet, double dtRemove, double dtTryGetFromCache, double dtFlush)
            => new WrapperRandomDelayAsync(store, dtStats, dtAdd, dtGet, dtRemove, dtTryGetFromCache, dtFlush);
    }
}
