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

        private class Header
        {
            public const int DefaultHeaderSizeInBytes = 1024;
            public const int DefaultIndexPageSizeInBytes = 64 * 1024;
            public static readonly Guid MagicBytesVersion1 = Guid.Parse("ff682f91-ad99-4135-a5d4-15ef97ed7cde");

            private readonly MemoryMappedViewAccessor m_accessor;
            private readonly long m_offset;

            // [ 0] 16 bytes
            public Guid MagicBytes
            {
                get { m_accessor.Read(m_offset, out Guid x); return x; }
                //set => m_accessor.Write(m_offset, ref value);
            }
            // [16] 4 bytes
            public int HeaderSizeInBytes
            {
                get => m_accessor.ReadInt32(m_offset + 16);
                //set => m_accessor.Write(m_offset + 16, value);
            }
            // [20] 4 bytes
            public int IndexPageSizeInBytes
            {
                get => m_accessor.ReadInt32(m_offset + 20);
                //set => m_accessor.Write(m_offset + 20, value);
            }
            // [24] 8 bytes
            public long TotalFileSize
            {
                get => m_accessor.ReadInt64(m_offset + 24);
                set => m_accessor.Write(m_offset + 24, value);
            }
            // [32] 8 bytes
            public long TotalIndexEntries
            {
                get => m_accessor.ReadInt64(m_offset + 32);
                set => m_accessor.Write(m_offset + 32, value);
            }
            // [40] 8 bytes
            public long DataCursorOffset
            {
                get => m_accessor.ReadInt64(m_offset + 40);
                set => m_accessor.Write(m_offset + 40, value);
            }
            // [48] 8 bytes
            public long IndexRootPageOffset
            {
                get => m_accessor.ReadInt64(m_offset + 48);
                set => m_accessor.Write(m_offset + 48, value);
            }
            // [56] 16 bytes (8 + 8)
            public DateTimeOffset Created
            {
                get => new(m_accessor.ReadInt64(m_offset + 56), new TimeSpan(m_accessor.ReadInt64(m_offset + 64)));
                //set { m_accessor.Write(m_offset + 56, Created.Ticks); m_accessor.Write(m_offset + 64, Created.Offset.Ticks); }
            }

            public Header(MemoryMappedViewAccessor accessor)
            {
                m_accessor = accessor;
                m_offset = m_accessor.ReadInt64(0L);

                var magicBuffer = new byte[16];
                if (m_accessor.ReadArray(m_offset, magicBuffer, 0, 16) != 16) throw new Exception("Reading header failed.");
                if (new Guid(magicBuffer) != MagicBytesVersion1) throw new Exception("Header does not start with magic bytes.");
            }

            /// <summary>
            /// Returns file size in bytes.
            /// </summary>
            public static void CreateEmptyDataFile(string dataFileName)
            {
                using var f = File.Open(dataFileName, FileMode.CreateNew, FileAccess.Write, FileShare.None);
                using var w = new BinaryWriter(f);

                var indexRootPageOffset = 8L + DefaultHeaderSizeInBytes;
                var cursorPos           = 8L + DefaultHeaderSizeInBytes + DefaultIndexPageSizeInBytes;
                var totalFileSize       = 8L + DefaultHeaderSizeInBytes + DefaultIndexPageSizeInBytes;
                var totalIndexEntries   = 0L;

                w.Write(8L);                                // header offset (in bytes)
                w.Write(MagicBytesVersion1.ToByteArray());  // MagicBytesVersion1
                w.Write(DefaultHeaderSizeInBytes);          // HeaderSizeInBytes
                w.Write(DefaultIndexPageSizeInBytes);       // IndexPageSizeInBytes
                w.Write(totalFileSize);                     // TotalFileSize
                w.Write(totalIndexEntries);                 // TotalIndexEntries
                w.Write(cursorPos);                         // CursorPos
                w.Write(indexRootPageOffset);               // IndexPos
                w.Write(DateTimeOffset.Now.Ticks);          // Created.Ticks
                w.Write(DateTimeOffset.Now.Offset.Ticks);   //        .Offset

                w.BaseStream.Position = indexRootPageOffset;
                w.Write(new byte[DefaultIndexPageSizeInBytes]);

                w.Flush();
            }
        }

        private readonly string m_dbDiskLocation;
        private readonly bool m_readOnlySnapshot;
        private string m_indexFilenameObsolete;
        private string m_dataFileName;

        private Dictionary<string, (long, int)> m_dbIndex;
        private Dictionary<string, WeakReference<object>> m_dbCache;
        private HashSet<object> m_dbCacheKeepAlive;
        private readonly HashSet<Type> m_typesToKeepAlive;
        private Header m_header;
        private MemoryMappedFile m_mmf;
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
                $"reserved space        : {m_header.TotalFileSize,20:N0} bytes",
                $"used space            : {m_header.DataCursorOffset,20:N0} bytes (including index)"
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
                        }

                        m_accessor.Dispose();
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

        #region Creation

        #region Constructors

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

        #endregion

        private void ParseDeprecatedIndexFile(string indexFileName)
        {
            Log("parsing deprecated index file ...");
            using var f = File.Open(indexFileName, FileMode.Open, FileAccess.Read, FileShare.Read);
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
                m_dbIndex = new Dictionary<string, (long, int)>(count);
                for (i = 0; i < count; i++)
                {
                    key = br.ReadString();
                    offset = br.ReadInt64();
                    size = br.ReadInt32();

                    m_dbIndex[key] = (offset, size);
                }
                sw.Stop();
                Log(
                    $"read existing index in {sw.Elapsed}",
                    $"that's appr. {(int)(count / sw.Elapsed.TotalSeconds):N0} entries/second"
                    );
            }
            catch (Exception e)
            {
                Log(
                    $"[CRITICAL ERROR] Damaged index file {indexFileName}",
                    $"[CRITICAL ERROR] Error a7814485-0e86-422e-92f0-9a4a31216a27.",
                    $"[CRITICAL ERROR] Could read {i:N0}/{count:N0} index entries.",
                    $"[CRITICAL ERROR] Last entry: {key} @ +{offset:N0} with size {size:N0} bytes.",
                    $"[CRITICAL ERROR] {e}"
                );
            }
        }

        private void Init()
        {
            m_indexFilenameObsolete = Path.Combine(m_dbDiskLocation, "index.bin");
            m_dataFileName  = Path.Combine(m_dbDiskLocation, "data.bin");

            if (!Directory.Exists(m_dbDiskLocation)) Directory.CreateDirectory(m_dbDiskLocation);

            if (!File.Exists(m_dataFileName))
            {
                Header.CreateEmptyDataFile(m_dataFileName);
                m_dbIndex = new Dictionary<string, (long, int)>();
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

            if (File.Exists(m_indexFilenameObsolete))
            {
                ParseDeprecatedIndexFile(m_indexFilenameObsolete);
                throw new NotImplementedException("Convert index to new format.");
            }

            var totalDataFileSizeInBytes = new FileInfo(m_dataFileName).Length;

            var mapName = m_dataFileName.ToMd5Hash().ToString();

            if (m_readOnlySnapshot)
            {
                try
                {
                    m_mmf = MemoryMappedFile.CreateFromFile(m_dataFileName, FileMode.Open, mapName, totalDataFileSizeInBytes, MemoryMappedFileAccess.Read);
                }
                catch
                {
                    m_mmf = MemoryMappedFile.OpenExisting(mapName, MemoryMappedFileRights.Read);
                }

                m_accessor = m_mmf.CreateViewAccessor(0, totalDataFileSizeInBytes, MemoryMappedFileAccess.Read);
                m_header = new (m_accessor);
            }
            else
            {
                m_mmf = MemoryMappedFile.CreateFromFile(m_dataFileName, FileMode.OpenOrCreate, mapName, totalDataFileSizeInBytes, MemoryMappedFileAccess.ReadWrite);
                m_accessor = m_mmf.CreateViewAccessor(0, totalDataFileSizeInBytes);
                m_header = new (m_accessor);
                Log(
                    $"reserved space        : {m_header.TotalFileSize,20:N0} bytes",
                    $"used space            : {m_header.DataCursorOffset,20:N0} bytes"
                    );
            }
        }

        #endregion

        #region Memory-mapped file

        private bool m_mmfIsClosedForResize = false;
        public bool SimulateFullDiskOnNextResize { get; set; }

        private void EnsureSpaceFor(long numberOfBytes)
        {
            if (SimulateFullDiskOnNextResize || m_header.DataCursorOffset + numberOfBytes > m_header.TotalFileSize)
            {
                try
                {
                    var newTotalFileSize = m_header.TotalFileSize;

                    if (newTotalFileSize < 1024 * 1024 * 1024)
                    {
                        newTotalFileSize *= 2;
                    }
                    else
                    {
                        newTotalFileSize += 1024 * 1024 * 1024;
                    }

                    Log($"resize data file to {newTotalFileSize / (1024 * 1024 * 1024.0):0.000} GiB");
                    lock (m_dbDiskLocation)
                    {
                        m_accessor.Dispose();
                        m_mmf.Dispose();

                        m_mmfIsClosedForResize = true;
                        ReOpenMemoryMappedFile(newTotalFileSize);
                    }
                }
                catch (Exception e)
                {
                    Log(
                        $"[CRITICAL ERROR] Exception occured while resizing disk store.",
                        $"[CRITICAL ERROR] Error ada9ed54-d748-4aef-b922-0bf93468fad8.",
                        $"[CRITICAL ERROR] {e}"
                    );

                    throw;
                }
            }
        }

        private void ReOpenMemoryMappedFile(long newCapacity)
        {
            lock (m_dbDiskLocation)
            {
                if (m_mmfIsClosedForResize)
                {
                    if (SimulateFullDiskOnNextResize)
                    {
                        var driveName = Path.GetFullPath(m_dataFileName)[0].ToString();
                        var freeSpaceInBytes = new DriveInfo(driveName).AvailableFreeSpace;
                        newCapacity = freeSpaceInBytes * 2; // force out of space
                    }

                    if (newCapacity == 0L)
                    {
                        newCapacity = new FileInfo(m_dataFileName).Length;
                    }

                    m_mmf = MemoryMappedFile.CreateFromFile(m_dataFileName, FileMode.OpenOrCreate, null, newCapacity, MemoryMappedFileAccess.ReadWrite);
                    m_accessor = m_mmf.CreateViewAccessor(0, newCapacity);
                    m_header = new (m_accessor);

                    m_mmfIsClosedForResize = false;
                }
            }
        }

        /// <summary>
        /// If mmf is closed because a previous resize failed due to insufficient disk space,
        /// this will retry to open the mmf (maybe there is more disk space now).
        /// </summary>
        private void EnsureMemoryMappedFileIsOpen()
        {
            if (m_mmfIsClosedForResize) ReOpenMemoryMappedFile(0L);
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
        public long GetUsedBytes() => m_header.DataCursorOffset;

        /// <summary>
        /// Total bytes reserved for blob storage.
        /// </summary>
        public long GetReservedBytes() => m_header.TotalFileSize;

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

            if (m_readOnlySnapshot) throw new InvalidOperationException("Read-only store does not support add.");

            Interlocked.Increment(ref m_stats.CountAdd);

            byte[] buffer = null;
            lock (m_dbCache)
            {
                m_dbCache[key] = new WeakReference<object>(value);
                if (m_typesToKeepAlive.Contains(value.GetType()))
                {
                    m_dbCacheKeepAlive.Add(value);
                    Interlocked.Increment(ref m_stats.CountKeepAlive);
                }
                buffer = getEncodedValue?.Invoke();
            }

            if (buffer == null) return;

            lock (m_dbDiskLocation)
            {
                EnsureMemoryMappedFileIsOpen();
                EnsureSpaceFor(numberOfBytes: buffer.Length);

                var cursorPos = m_header.DataCursorOffset;
                m_accessor.WriteArray(cursorPos, buffer, 0, buffer.Length);
                m_dbIndex[key] = (cursorPos, buffer.Length);
                m_indexHasChanged = true;
                m_header.DataCursorOffset = cursorPos + buffer.Length;

                LatestKeyAdded = key;
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
                EnsureMemoryMappedFileIsOpen();
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
                EnsureMemoryMappedFileIsOpen();
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
                EnsureMemoryMappedFileIsOpen();
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
                EnsureMemoryMappedFileIsOpen();
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
                EnsureMemoryMappedFileIsOpen();
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
                lock (m_dbDiskLocation)
                {
                    EnsureMemoryMappedFileIsOpen();
                    m_accessor.Flush();
                }
            }
        }

#if false
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
                            $"      total number of keys: {m_dbIndex.Count:N0}",
                            $"      latest key added    : {latestKeyAdded}"
                            );

                        if (!m_indexHasChanged) return;
                        using (var f = File.Open(m_indexFilenameObsolete, FileMode.Create, FileAccess.Write, FileShare.Read))
                        using (var bw = new BinaryWriter(f))
                        {
                            bw.Write(m_dbIndex.Count);
                            foreach (var kv in m_dbIndex)
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
                            $"      total number of keys: {m_dbIndex.Count:N0}",
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
#endif
    }
}
