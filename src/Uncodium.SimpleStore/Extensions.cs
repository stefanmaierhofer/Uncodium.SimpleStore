/*
   MIT License
   
   Copyright (c) 2014,2015,2016,2017,2018,2019,2020,2021,2022,2023 Stefan Maierhofer.
   
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
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Uncodium.SimpleStore;

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
    /// Store UTF8-encoded value.
    /// </summary>
    public static void Add(this ISimpleStore store, string key, string value)
        => store.Add(key, Encoding.UTF8.GetBytes(value));

    /// <summary>
    /// Store UTF8-encoded value.
    /// </summary>
    public static Task AddAsync(this ISimpleStoreAsync store, string key, string value)
        => store.AddAsync(key, Encoding.UTF8.GetBytes(value));

    /// <summary>
    /// Each store operation fails with given probability.
    /// </summary>
    public static ISimpleStore FailRandomly(this ISimpleStore store, double pFail)
        => new WrapperRandomFail(store, pFail);

    /// <summary>
    /// Each store operation fails with given probability.
    /// </summary>
    public static ISimpleStore FailRandomly(this ISimpleStore store, double pStats, double pAdd, double pGet, double pRemove, double pFlush)
        => new WrapperRandomFail(store, pStats, pAdd, pGet, pRemove, pFlush);

    /// <summary>
    /// Each store operation is delayed between 0 and given duration in seconds.
    /// </summary>
    public static ISimpleStore DelayRandomly(this ISimpleStore store, double dt)
        => new WrapperRandomDelay(store, dt);

    /// <summary>
    /// Each store operation is delayed between 0 and given duration in seconds.
    /// </summary>
    public static ISimpleStore DelayRandomly(this ISimpleStore store, double dtStats, double dtAdd, double dtContains, double dtGet, double dtRemove, double dtFlush)
        => new WrapperRandomDelay(store, dtStats, dtAdd, dtContains, dtGet, dtRemove, dtFlush);
}

