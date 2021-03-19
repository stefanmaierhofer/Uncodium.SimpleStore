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
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// A memory-mapped key/value store on disk.
    /// </summary>
    public class SimpleDiskStore : ISimpleStore
    {
        #region Private

        private readonly string m_dbDiskLocation;
        private readonly bool m_readOnlySnapshot;
        private string m_indexFilename;
        private string m_dataFilename;

        private class Index
        {
            private Dictionary<string, (long, int)> _current = new();
            private readonly List<Dictionary<string, (long, int)>> _index = new();

            public void Add(string key, (long, int) value)
            {
                if (_current.Count >= 32 * 1024 * 1024)
                {
                    Console.WriteLine("add index chunk");
                    _index.Add(_current);
                    _current = new();
                }

                _current.Add(key, value);
            }

            public int Count => _current.Count + _index.Sum(x => x.Count);

            public string[] Keys
            {
                get
                {
                    var rs = new List<string>(_current.Keys);
                    foreach (var i in _index) rs.AddRange(i.Keys);
                    return rs.ToArray();
                }
            }

            public bool ContainsKey(string key)
            {
                if (_current.ContainsKey(key)) return true;
                foreach (var x in _index) if (x.ContainsKey(key)) return true;
                return false;
            }

            public bool Remove(string key)
            {
                if (_current.Remove(key)) return true;
                foreach (var x in _index) if (x.Remove(key)) return true;
                return false;
            }

            public bool TryGetValue(string key, out (long,int) result)
            {
                if (_current.TryGetValue(key, out result)) return true;
                foreach (var x in _index) if (x.TryGetValue(key, out result)) return true;
                return false;
            }

            public IEnumerable<KeyValuePair<string,(long,int)>> EnumerateEntries()
            {
                foreach (var kv in _current) yield return kv;
                foreach (var x in _index) foreach (var kv in x) yield return kv;
            }
        }
        private Index m_dbIndex;

        private Dictionary<string, WeakReference<object>> m_dbCache;
        private HashSet<object> m_dbCacheKeepAlive;
        private readonly HashSet<Type> m_typesToKeepAlive;
        private long m_dataSize;
        private long m_dataPos;
        private MemoryMappedFile m_mmf;
        private MemoryMappedViewAccessor m_accessorSize;
        private MemoryMappedViewAccessor m_accessor;

        private Stats m_stats;

        private bool m_indexHasChanged = false;
        private readonly CancellationTokenSource m_cts = new();
        private readonly SemaphoreSlim m_flushIndexSemaphore = new(1);

        #endregion

        #region Disposal

        private bool m_isDisposed = false;
        private string m_disposeStackTrace = null;
        private bool m_loggedDisposeStackTrace = false;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckDisposed() 
        {
            if (m_isDisposed)
            {
                if (!m_loggedDisposeStackTrace)
                {
                    Log($"Trying to dispose store, that has already been disposed at {m_disposeStackTrace}");
                    m_loggedDisposeStackTrace = true;
                }
                throw new ObjectDisposedException(nameof(SimpleDiskStore));
            }
        }

        public void Dispose()
        {
            CheckDisposed();

            var token = Guid.NewGuid();
            if (!m_readOnlySnapshot) Log(
                $"",
                $"shutdown {token} (begin)",
                $"reserved space        : {m_dataSize,20:N0} bytes",
                $"used space            : {m_dataPos,20:N0} bytes"
                );

            while (true)
            {
                if (m_flushIndexSemaphore.Wait(1000))
                {
                    try
                    {
                        CheckDisposed();

                        if (!m_readOnlySnapshot)
                        {
                            m_accessor.Flush();
                            m_accessorSize.Flush();
                            FlushIndex(isDisposing: true);
                        }

                        m_accessor.Dispose();
                        m_accessorSize.Dispose();
                        m_mmf.Dispose();
                        m_cts.Cancel();

                        Log(
                            $"shutdown {token} - latest known key is {LatestKeyAdded},",
                            $"shutdown {token} - should be the same key as indicated above in \"(2/2) flush index to disk (end)\"",
                            $"shutdown {token} (end)"
                            );

                        return;
                    }
                    finally
                    {
                        m_flushIndexSemaphore.Release();
                        m_disposeStackTrace = Environment.StackTrace;
                        m_isDisposed = true;
                    }
                }
                else
                {
                    Log($"shutdown {token} is waiting for index being flushed to disk");
                }
            }
        }


        #endregion

        #region Logging

        private readonly Action<string[]> f_logLines = null;

        private void Log(params string[] lines)
        {
            if (!m_readOnlySnapshot)
            {
                lock (f_logLines)
                {
                    f_logLines(lines);
                }
            }

#if DEBUG
            foreach (var line in lines)
            {
                Console.WriteLine($"[Uncodium.SimpleDiskStore][{DateTimeOffset.Now}] {line}");
            }
#endif
        }

        #endregion

        #region Construction

        /// <summary>
        /// Creates store in folder 'dbDiskLocation'.
        /// Optional set of types that will be kept alive in memory.
        /// Optionally opens current state read-only.
        /// Optionally a logger can be supplied which replaces the default logger to log.txt.
        /// </summary>
        private SimpleDiskStore(string dbDiskLocation, Type[] typesToKeepAlive, bool readOnlySnapshot, Action<string[]> logLines)
        {
            m_dbDiskLocation = dbDiskLocation;
            m_readOnlySnapshot = readOnlySnapshot;

            if (typesToKeepAlive == null) typesToKeepAlive = new Type[0];
            m_typesToKeepAlive = new HashSet<Type>(typesToKeepAlive);

            if (logLines != null)
            {
                f_logLines = logLines;
            }
            else
            {
                var logFilename = Path.Combine(m_dbDiskLocation, "log.txt");
                f_logLines = lines => File.AppendAllLines(logFilename, lines.Select(line => $"[{DateTimeOffset.Now}] {line}"));
            }

            Init();
        }

        /// <summary>
        /// Creates store in folder 'dbDiskLocation'.
        /// Optional set of types that will be kept alive in memory.
        /// Optionally opens current state read-only.
        /// </summary>
        private SimpleDiskStore(string dbDiskLocation, Type[] typesToKeepAlive, bool readOnlySnapshot)
            : this(dbDiskLocation, typesToKeepAlive, readOnlySnapshot: readOnlySnapshot, logLines: null)
        { }

        /// <summary>
        /// Creates store in folder 'dbDiskLocation'.
        /// Optional set of types that will be kept alive in memory.
        /// </summary>
        public SimpleDiskStore(string dbDiskLocation, Type[] typesToKeepAlive) 
            : this(dbDiskLocation, typesToKeepAlive, readOnlySnapshot: false, logLines: null)
        { }

        /// <summary>
        /// Creates store in folder 'dbDiskLocation'.
        /// Optional set of types that will be kept alive in memory.
        /// Optionally a logger can be supplied which replaces the default logger to log.txt.
        /// </summary>
        public SimpleDiskStore(string dbDiskLocation, Type[] typesToKeepAlive, Action<string[]> logLines)
            : this(dbDiskLocation, typesToKeepAlive, readOnlySnapshot: false, logLines: logLines)
        { }

        /// <summary>
        /// Creates store in folder 'dbDiskLocation'.
        /// </summary>
        public SimpleDiskStore(string dbDiskLocation) 
            : this(dbDiskLocation, typesToKeepAlive: null, readOnlySnapshot: false, logLines: null) 
        { }

        /// <summary>
        /// Creates store in folder 'dbDiskLocation'.
        /// Optionally a logger can be supplied which replaces the default logger to log.txt.
        /// </summary>
        public SimpleDiskStore(string dbDiskLocation, Action<string[]> logLines)
            : this(dbDiskLocation, typesToKeepAlive: null, readOnlySnapshot: false, logLines: logLines)
        { }

        /// <summary>
        /// Opens store in folder 'dbDiskLocation' in read-only snapshot mode.
        /// This means that no store entries that are added after the call to OpenReadOnlySnapshot will be(come) visible.
        /// Optional set of types that will be kept alive in memory.
        /// </summary>
        public static SimpleDiskStore OpenReadOnlySnapshot(string dbDiskLocation, Type[] typesToKeepAlive)
            => new(dbDiskLocation, typesToKeepAlive, readOnlySnapshot: true);

        /// <summary>
        /// Opens store in folder 'dbDiskLocation' in read-only snapshot mode.
        /// This means that no store entries that are added after the call to OpenReadOnlySnapshot will be(come) visible.
        /// </summary>
        public static SimpleDiskStore OpenReadOnlySnapshot(string dbDiskLocation)
            => new(dbDiskLocation, typesToKeepAlive: null, readOnlySnapshot: true);

        private void Init()
        {
            m_indexFilename = Path.Combine(m_dbDiskLocation, "index.bin");
            m_dataFilename  = Path.Combine(m_dbDiskLocation, "data.bin");

            var dataFileIsNewlyCreated = false;
            if (!Directory.Exists(m_dbDiskLocation)) Directory.CreateDirectory(m_dbDiskLocation);
            if (!File.Exists(m_dataFilename))
            {
                File.WriteAllBytes(m_dataFilename, new byte[0]);
                dataFileIsNewlyCreated = true;
            }

            Log(
                $"",
                $"====================================",
                $"  starting up (version {Global.Version})",
                $"====================================",
                $""
                );

            m_dbCache = new Dictionary<string, WeakReference<object>>();
            m_dbCacheKeepAlive = new HashSet<object>();
            if (File.Exists(m_indexFilename))
            {
                Log("read existing index ...");
                using var f = File.Open(m_indexFilename, FileMode.Open, FileAccess.Read, FileShare.Read);
                using var br = new BinaryReader(f);
                var count = 0;
                var i = 0;
                var key = string.Empty;
                var offset = 0L;
                var size = 0;
                try
                {
                    count = br.ReadInt32();
                    Log($"entries: {count:N0}");
                    var sw = new Stopwatch(); sw.Start();
                    m_dbIndex = new();
                    for (i = 0; i < count; i++)
                    {
                        key = br.ReadString();
                        offset = br.ReadInt64();
                        size = br.ReadInt32();

                        m_dbIndex.Add(key, (offset, size));
                    }
                    sw.Stop();
                    Log(
                        $"read existing index in {sw.Elapsed}",
                        $"that's appr. {(int)(count/sw.Elapsed.TotalSeconds):N0} entries/second"
                        );
                }
                catch (Exception e)
                {
                    Log(
                        $"[CRITICAL ERROR] Damaged index file {m_indexFilename}",
                        $"[CRITICAL ERROR] Error a7814485-0e86-422e-92f0-9a4a31216a27.",
                        $"[CRITICAL ERROR] Could read {i:N0}/{count:N0} index entries.",
                        $"[CRITICAL ERROR] Last entry: {key} @ +{offset:N0} with size {size:N0} bytes.",
                        $"[CRITICAL ERROR] {e}"
                    );
                }
            }
            else
            {
                m_dbIndex = new();

                using var f = File.Open(m_indexFilename, FileMode.Create, FileAccess.Write, FileShare.Read);
                using var bw = new BinaryWriter(f);
                bw.Write(m_dbIndex.Count);
            }

            m_dataSize = new FileInfo(m_dataFilename).Length;
            if (m_dataSize == 0) m_dataSize = 1024 * 1024; else m_dataSize -= 8;

            var mapName = m_dataFilename.ToMd5Hash().ToString();

            if (m_readOnlySnapshot)
            {
                try
                {
                    m_mmf = MemoryMappedFile.CreateFromFile(m_dataFilename, FileMode.Open, mapName, 8 + m_dataSize, MemoryMappedFileAccess.Read);
                }
                catch
                {
                    m_mmf = MemoryMappedFile.OpenExisting(mapName, MemoryMappedFileRights.Read);
                }

                m_accessorSize = m_mmf.CreateViewAccessor(0, 8, MemoryMappedFileAccess.Read);
                m_accessor = m_mmf.CreateViewAccessor(8, m_dataSize, MemoryMappedFileAccess.Read);
                m_dataPos = 0L;
            }
            else
            {
                m_mmf = MemoryMappedFile.CreateFromFile(m_dataFilename, FileMode.OpenOrCreate, mapName, 8 + m_dataSize, MemoryMappedFileAccess.ReadWrite);
                m_accessorSize = m_mmf.CreateViewAccessor(0, 8);
                m_accessor = m_mmf.CreateViewAccessor(8, m_dataSize);
                m_dataPos = dataFileIsNewlyCreated ? 0L : m_accessorSize.ReadInt64(0);
                Log(
                    $"reserved space        : {m_dataSize,20:N0} bytes",
                    $"used space            : {m_dataPos,20:N0} bytes"
                    );
            }
        }

        #endregion

        /// <summary>
        /// Various counts and other statistics.
        /// </summary>
        public Stats Stats => m_stats;

        /// <summary>
        /// Gets latest key added to the store.
        /// May not yet have been flused to disk.
        /// </summary>
        public string LatestKeyAdded { get; private set; }

        /// <summary>
        /// Gets latest key flushed to disk.
        /// </summary>
        public string LatestKeyFlushed { get; private set; }

        /// <summary>
        /// Total bytes used for blob storage.
        /// </summary>
        public long GetUsedBytes() => m_dataPos;

        /// <summary>
        /// Total bytes reserved for blob storage.
        /// </summary>
        public long GetReservedBytes() => m_dataSize;

        /// <summary>
        /// Current version.
        /// </summary>
        public string Version => Global.Version;

        /// <summary>
        /// Adds key/value pair to store.
        /// If 'getEncodedValue' is null, than value will not be written to disk.
        /// </summary>
        public void Add(string key, object value, Func<byte[]> getEncodedValue)
        {
            CheckDisposed();

            var debugOldDbIndexSize = m_dbIndex.Count;
            var debugOldDbCacheSize = m_dbCache.Count;
            int? debugBufferSize = -1;
            int debugStepReached = -1;

            try
            {
                if (m_readOnlySnapshot) throw new InvalidOperationException("Read-only store does not support add.");
                debugStepReached = 1;

                Interlocked.Increment(ref m_stats.CountAdd);
                debugStepReached = 2;

                byte[] buffer = null;
                debugStepReached = 3;

                lock (m_dbCache)
                {
                    debugStepReached = 4;

                    if (m_dbCache.Count > 1024 * 1024) m_dbCache.Clear();
                    m_dbCache[key] = new WeakReference<object>(value);
                    debugStepReached = 5;

                    if (m_typesToKeepAlive.Contains(value.GetType()))
                    {
                        debugStepReached = 6;

                        m_dbCacheKeepAlive.Add(value);
                        debugStepReached = 7;

                        Interlocked.Increment(ref m_stats.CountKeepAlive);
                        debugStepReached = 8;
                    }

                    buffer = getEncodedValue?.Invoke();
                    debugStepReached = 9;

                    debugBufferSize = buffer?.Length;
                    debugStepReached = 91;
                }

                if (buffer == null) return;
                debugStepReached = 10;

                lock (m_dbDiskLocation)
                {
                    debugStepReached = 11;

                    if (m_dataPos + buffer.Length > m_dataSize)
                    {
                        debugStepReached = 12;
                        try
                        {
                            debugStepReached = 13;

                            if (m_dataSize < 1024 * 1024 * 1024)
                            {
                                debugStepReached = 14;

                                m_dataSize *= 2;
                                debugStepReached = 15;
                            }
                            else
                            {
                                debugStepReached = 16;

                                m_dataSize += 1024 * 1024 * 1024;
                                debugStepReached = 17;
                            }

                            Log($"resize data file to {m_dataSize / (1024 * 1024 * 1024.0):0.000} GiB");
                            debugStepReached = 18;

                            m_accessorSize.Dispose();
                            debugStepReached = 19;

                            m_accessor.Dispose();
                            debugStepReached = 20;

                            m_mmf.Dispose();
                            debugStepReached = 21;

                            m_mmf = MemoryMappedFile.CreateFromFile(m_dataFilename, FileMode.OpenOrCreate, null, 8 + m_dataSize, MemoryMappedFileAccess.ReadWrite);
                            debugStepReached = 22;

                            m_accessorSize = m_mmf.CreateViewAccessor(0, 8);
                            debugStepReached = 23;

                            m_accessor = m_mmf.CreateViewAccessor(8, m_dataSize);
                            debugStepReached = 24;
                        }
                        catch (Exception e)
                        {
                            debugStepReached = 25;

                            Log(
                                $"[CRITICAL ERROR] Exception occured while resizing disk store.",
                                $"[CRITICAL ERROR] Error ada9ed54-d748-4aef-b922-0bf93468fad8.",
                                $"[CRITICAL ERROR] {e}"
                            );
                            debugStepReached = 26;

                            throw;
                        }
                    }


                    debugStepReached = 27;

                    m_accessor.WriteArray(m_dataPos, buffer, 0, buffer.Length);
                    debugStepReached = 28;

                    m_dbIndex.Add(key, (m_dataPos, buffer.Length));
                    debugStepReached = 29;

                    m_indexHasChanged = true;
                    debugStepReached = 30;

                    m_dataPos += buffer.Length;
                    debugStepReached = 31;

                    m_accessorSize.Write(0, m_dataPos);
                    debugStepReached = 32;

                    LatestKeyAdded = key;
                    debugStepReached = 33;
                }
            }
            catch (Exception e)
            {
                var msg = $"Add({key}, {value?.GetType()}) failed. Reached step {debugStepReached}. ";

                try
                {
                    debugStepReached = 35;
                    msg += $"Index size before 'add' was {debugOldDbIndexSize}. ";
                    debugStepReached = 36;
                    msg += $"Cache size before 'add' was {debugOldDbCacheSize}. ";
                    debugStepReached = 37;
                    msg += $"Buffer size is {debugBufferSize}. ";
                    debugStepReached = 38;
                    msg += $"Environment.WorkingSet={Environment.WorkingSet}. ";
                    debugStepReached = 39;
                    msg += $"GC.GetTotalMemory(false) = {GC.GetTotalMemory(false)}. ";
                    debugStepReached = 40;
                    msg += $"m_dataPos = {m_dataPos}. ";
                    debugStepReached = 41;
                    msg += $"m_dataSize = {m_dataSize}. ";
                    debugStepReached = 42;
                    msg += $"m_dataFilename = {m_dataFilename}. ";
                    debugStepReached = 43;
                    msg += $"Reached step {debugStepReached}. ";
                    debugStepReached = 44;

                    throw new Exception(msg, innerException: e);
                }
                catch (Exception e2)
                {
                    debugStepReached = 45;
                    throw new Exception(msg + $"!!!Reached step {debugStepReached}. Second exception is {e2}. ", innerException: e);
                }
            }
        }

        /// <summary>
        /// True if key is contained in store.
        /// </summary>
        public bool Contains(string key)
        {
            CheckDisposed();

            bool result;
            lock (m_dbDiskLocation)
            {
                result = m_dbIndex.ContainsKey(key);
                m_indexHasChanged = true;
            }
            Interlocked.Increment(ref m_stats.CountContains);
            return result;
        }

        /// <summary>
        /// Get value from key,
        /// or null if key does not exist.
        /// </summary>
        public byte[] Get(string key)
        {
            CheckDisposed();

            lock (m_dbDiskLocation)
            {
                if (m_dbIndex.TryGetValue(key, out (long offset, int size) entry))
                {
                    try
                    {
                        var buffer = new byte[entry.size];
                        var readcount = m_accessor.ReadArray(entry.offset, buffer, 0, buffer.Length);
                        if (readcount != buffer.Length) throw new InvalidOperationException();
                        Interlocked.Increment(ref m_stats.CountGet);
                        return buffer;
                    }
                    catch (Exception e)
                    {
                        var count = Interlocked.Increment(ref m_stats.CountGetWithException);
                        Log($"[CRITICAL ERROR] Get(key={key}) failed.",
                            $"[CRITICAL ERROR] entry = {{offset={entry.offset}, size={entry.size}}}",
                            $"[CRITICAL ERROR] So far, {count} Get-calls failed.",
                            $"[CRITICAL ERROR] exception = {e}"
                            );
                        return null;
                    }
                }
                else
                {
                    Interlocked.Increment(ref m_stats.CountGetInvalidKey);
                    return null;
                }
            }
        }

        /// <summary>
        /// Get slice of value from key,
        /// or null if key does not exist.
        /// </summary>
        public byte[] GetSlice(string key, long offset, int length)
        {
            CheckDisposed();

            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), "Offset must be greater or equal 0.");
            if (length < 1) throw new ArgumentOutOfRangeException(nameof(offset), "Size must be greater than 0.");

            lock (m_dbDiskLocation)
            {
                if (m_dbIndex.TryGetValue(key, out (long offset, int size) entry))
                {
                    if (offset >= entry.size) throw new ArgumentOutOfRangeException(nameof(offset), "Offset must be less than length of value buffer.");
                    if (offset + length > entry.size) throw new ArgumentOutOfRangeException(nameof(offset), "Offset + size exceeds length of value buffer.");

                    try
                    {
                        var buffer = new byte[length];
                        var readcount = m_accessor.ReadArray(entry.offset + offset, buffer, 0, length);
                        if (readcount != length) throw new InvalidOperationException();
                        Interlocked.Increment(ref m_stats.CountGetSlice);
                        return buffer;
                    }
                    catch (Exception e)
                    {
                        var count = Interlocked.Increment(ref m_stats.CountGetSliceWithException);
                        Log($"[CRITICAL ERROR] GetSlice(key={key}, offset={offset}, length={length}) failed.",
                            $"[CRITICAL ERROR] entry = {{offset={entry.offset}, size={entry.size}}}",
                            $"[CRITICAL ERROR] So far, {count} GetSlice-calls failed.",
                            $"[CRITICAL ERROR] exception = {e}"
                            );
                        return null;
                    }
                }
                else
                {
                    Interlocked.Increment(ref m_stats.CountGetInvalidKey);
                    return null;
                }
            }
        }

        /// <summary>
        /// Get read stream for value from key.
        /// This is not thread-safe with respect to overwriting or removing existing values.
        /// </summary>
        public Stream OpenReadStream(string key)
        {
            CheckDisposed();

            lock (m_dbDiskLocation)
            {
                if (m_dbIndex.TryGetValue(key, out (long, int) entry))
                {
                    return m_mmf.CreateViewStream(8 + entry.Item1, entry.Item2, MemoryMappedFileAccess.Read);
                }
                else
                {
                    Interlocked.Increment(ref m_stats.CountGetInvalidKey);
                    return null;
                }
            }
        }

        /// <summary>
        /// Remove entry.
        /// </summary>
        public void Remove(string key)
        {
            CheckDisposed();

            if (m_readOnlySnapshot) throw new InvalidOperationException("Read-only store does not support remove.");
            lock (m_dbDiskLocation)
            {
                m_dbIndex.Remove(key);
                m_indexHasChanged = true;
            }
            Interlocked.Increment(ref m_stats.CountRemove);
        }

        /// <summary>
        /// Returns decoded value from cache, or null if not available.
        /// </summary>
        public object TryGetFromCache(string key)
        {
            CheckDisposed();

            lock (m_dbCache)
            {
                if (m_dbCache.TryGetValue(key, out WeakReference<object> weakRef))
                {
                    if (weakRef.TryGetTarget(out object data))
                    {
                        Interlocked.Increment(ref m_stats.CountGetCacheHit);
                        return data;
                    }
                }
                Interlocked.Increment(ref m_stats.CountGetCacheMiss);
                return null;
            }
        }

        /// <summary>
        /// Gets a snapshot of all existing keys.
        /// </summary>
        public string[] SnapshotKeys()
        {
            lock (m_dbDiskLocation)
            {
                Interlocked.Increment(ref m_stats.CountSnapshotKeys);
                return m_dbIndex.Keys.ToArray();
            }
        }

        /// <summary>
        /// Commit pending changes to storage.
        /// </summary>
        public void Flush()
        {
            CheckDisposed();

            if (m_readOnlySnapshot)
            {
                return;
            }
            else
            {
                m_accessor.Flush();
                m_accessorSize.Flush();
                FlushIndex();
            }
        }

        private void FlushIndex(bool isDisposing = false)
        {
            CheckDisposed();

            if (m_readOnlySnapshot) return;

            if (isDisposing || m_flushIndexSemaphore.Wait(0))
            {
                lock (m_dbDiskLocation)
                {
                    try
                    {
                        CheckDisposed();

                        var sw = new Stopwatch();
                        sw.Start();

                        var latestKeyAdded = LatestKeyAdded;
                        Log(
                            $"(1/2) flush index to disk (begin)",
                            $"      total number of keys: {(int)m_dbIndex.Count:N0}",
                            $"      latest key added    : {latestKeyAdded}"
                            );

                        if (!m_indexHasChanged) return;
                        using (var f = File.Open(m_indexFilename, FileMode.Create, FileAccess.Write, FileShare.Read))
                        using (var bw = new BinaryWriter(f))
                        {
                            bw.Write((int)m_dbIndex.Count);
                            foreach (var kv in m_dbIndex.EnumerateEntries())
                            {
                                bw.Write(kv.Key);
                                bw.Write(kv.Value.Item1);
                                bw.Write(kv.Value.Item2);
                            }
                        }
                        m_indexHasChanged = false;
                        Log(
                            $"(2/2) flush index to disk (end)",
                            $"      total duration      : {sw.Elapsed}",
                            $"      total number of keys: {(int)m_dbIndex.Count:N0}",
                            $"      latest key added    : {LatestKeyAdded}"
                            );

                        if (latestKeyAdded != LatestKeyAdded)
                        {
                            Log(
                                $"[CRITICAL ERROR] One or more keys have been added while flushing index to file.",
                                $"[CRITICAL ERROR] Error fc85712e-26b1-414a-97eb-22faf87c5718."
                                );
                        }

                        LatestKeyFlushed = latestKeyAdded;
                    }
                    catch (Exception e)
                    {
                        Log(
                            $"[CRITICAL ERROR] Exception occured while flushing index to disk.",
                            $"[CRITICAL ERROR] Error 150dffaa-982f-43a9-b9e4-db8bf6116296.",
                            $"[CRITICAL ERROR] {e}"
                        );
                        throw;
                    }
                    finally
                    {
                        if (!isDisposing) m_flushIndexSemaphore.Release();
                    }
                }
            }
            else
            {
                Log($"[WARNING] Concurrent flush attempt detected. Warning dff7d6ca-6451-4258-993a-e1448e58e3b7.");
            }
        }
    }
}
