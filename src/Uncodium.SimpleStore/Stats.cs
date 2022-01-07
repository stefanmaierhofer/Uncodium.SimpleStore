/*
   MIT License
   
   Copyright (c) 2014,2015,2016,2017,2018,2019,2020,2021,2022 Stefan Maierhofer.
   
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

namespace Uncodium.SimpleStore;

public struct Stats
{
    public long CountAdd;
    public long CountContains;
    public long CountGet;
    public long CountGetWithException;
    public long CountGetSlice;
    public long CountGetSliceWithException;
    public long CountGetStream;
    public long CountGetInvalidKey;
    public long CountRemove;
    public long CountRemoveInvalidKey;
    public long CountList;
    public long CountFlush;

    /// <summary>
    /// Latest key added to the store.
    /// </summary>
    public string LatestKeyAdded;

    /// <summary>
    /// Latest key flushed to backing storage.
    /// In SimpleDiskStore and SimpleFolderStore this is a file on disk.
    /// In SimpleMemoryStore this is memory, so LatestKeyFlushed is always identical to LatestKeyAdded.
    /// </summary>
    public string LatestKeyFlushed;

    public Stats Copy() => new()
    {
        CountAdd = CountAdd,
        CountContains = CountContains,
        CountGet = CountGet,
        CountGetWithException = CountGetWithException,
        CountGetSlice = CountGetSlice,
        CountGetSliceWithException = CountGetSliceWithException,
        CountGetStream = CountGetStream,
        CountGetInvalidKey = CountGetInvalidKey,
        CountRemove = CountRemove,
        CountRemoveInvalidKey = CountRemoveInvalidKey,
        CountList = CountList,
        CountFlush = CountFlush
    };
}

