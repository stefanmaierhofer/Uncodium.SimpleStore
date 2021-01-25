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
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// A memory-mapped key/value store on disk.
    /// </summary>
    public class SimpleDiskStore : ISimpleStore
    {
        private const int MAX_KEY_LENGTH = 4096;

        #region Private

        private struct Position
        {
            public long Value;
            public Position(long value) => Value = value;
            public static Position operator +(Position self, Offset32 offset) => new(self.Value + offset.Value);
            public static implicit operator long(Position self) => self.Value;

            public override string ToString() => $"@{Value:N0}";
        }
        private struct Offset32
        {
            public int Value;
            public Offset32(int value) => Value = value;
            public static implicit operator Offset32(int self) => new(self);
            public override string ToString() => $"+{Value:N0}";
        }
        private class FieldInt16
        {
            public Offset32 Offset;
            public FieldInt16(Offset32 offset) => Offset = offset;
            public void Write(MemoryMappedViewAccessor accessor, Block block, short value)
            {
                var p = block.Min.Value + Offset.Value;
                if (p + 2 > block.Max.Value) throw new Exception("Field outside of block.");
                accessor.Write(p, value);
            }
            public short Read(MemoryMappedViewAccessor accessor, Block block)
            {
                var p = block.Min.Value + Offset.Value;
                if (p + 2 > block.Max.Value) throw new Exception("Field outside of block.");
                return accessor.ReadInt16(p);
            }
        }
        private class FieldInt32
        {
            public Offset32 Offset;
            public FieldInt32(Offset32 offset) => Offset = offset;
            public void Write(MemoryMappedViewAccessor accessor, Block block, int value)
            {
                var p = block.Min.Value + Offset.Value;
                if (p + 4 > block.Max.Value) throw new Exception("Field outside of block.");
                accessor.Write(p, value);
            }
            public int Read(MemoryMappedViewAccessor accessor, Block block)
            {
                var p = block.Min.Value + Offset.Value;
                if (p + 4 > block.Max.Value) throw new Exception("Field outside of block.");
                return accessor.ReadInt32(p);
            }
        }
        private class FieldInt64
        {
            public Offset32 Offset;
            public FieldInt64(Offset32 offset) => Offset = offset;
            public void Write(MemoryMappedViewAccessor accessor, Block block, long value)
            {
                var p = block.Min.Value + Offset.Value;
                if (p + 8 > block.Max.Value) throw new Exception("Field outside of block.");
                accessor.Write(p, value);
            }
            public long Read(MemoryMappedViewAccessor accessor, Block block)
            {
                var p = block.Min.Value + Offset.Value;
                if (p + 8 > block.Max.Value) throw new Exception("Field outside of block.");
                return accessor.ReadInt64(p);
            }
        }
        private class FieldBuffer
        {
            public Offset32 Offset;
            public FieldBuffer(Offset32 offset) => Offset = offset;
            public void Write(MemoryMappedViewAccessor accessor, Block block, byte[] value)
            {
                var p = block.Min.Value + Offset.Value;
                if (p + value.Length > block.Max.Value) throw new Exception("Field outside of block.");
                accessor.WriteArray(p, value, 0, value.Length);
            }
            public byte[] Read(MemoryMappedViewAccessor accessor, Block block, int length)
            {
                var p = block.Min.Value + Offset.Value;
                if (p + length > block.Max.Value) throw new Exception("Field outside of block.");
                var buffer = new byte[length];
                if (accessor.ReadArray(p, buffer, 0, length) != length) throw new Exception("Failed to read buffer.");
                return buffer;
            }
        }
        private struct Block
        {
            public Position Min;
            public Position Max;
            public Block(Position min, Offset32 size)
            {
                Min = min;
                Max = min + size;
            }
            public Offset32 Size => new((int)(Max.Value - Min.Value));
            public static Position operator +(Block self, Offset32 offset) => new(self.Min.Value + offset.Value);
            public override string ToString() => $"[{Min.Value:N0}, {Max.Value:N0}]";
        }

        private class IndexPage
        {
            public static readonly Guid MAGIC_VALUE = Guid.Parse("5587b343-403d-4f62-af22-a18da686b1da");
            private static readonly FieldBuffer FIELD_MAGIC  = new( 0);  // +16
            private static readonly FieldInt64  FIELD_NEXT   = new(16);  //  +8
            private static readonly FieldInt32  FIELD_CURSOR = new(24);  //  +4
            private static readonly FieldInt32  FIELD_COUNT  = new(28);  //  +4
            private static readonly Offset32    FIELD_DATA   = new(32);

            private readonly MemoryMappedViewAccessor m_accessor;
            public Block Range { get; }

            public IndexPage(IndexPage x) => new IndexPage(x.m_accessor, x.Range);
            
            public IndexPage(MemoryMappedViewAccessor accessor, Block range)
            {
                m_accessor = accessor;
                Range = range;
            }

            public static IndexPage Init(MemoryMappedViewAccessor accessor, Block range, IndexPage next)
            {
                var cursor = range + FIELD_DATA;
                var result = new IndexPage(accessor, range)
                {
                    Magic = MAGIC_VALUE,
                    Next = next,
                    Cursor = cursor,
                    Count = 0
                };

                if (result.Cursor.Value < result.Range.Min.Value || result.Cursor.Value > result.Range.Max.Value) Debugger.Break();

                return result;
            }

            public bool TryAdd(string key, (long offset, int size) value)
            {
                if (key.Length > MAX_KEY_LENGTH)
                    throw new ArgumentOutOfRangeException(nameof(key), $"Max key length is {MAX_KEY_LENGTH}, but key has {key.Length}.");

                var keyBuffer = Encoding.UTF8.GetBytes(key);
                if (keyBuffer.Length > MAX_KEY_LENGTH)
                    throw new ArgumentOutOfRangeException(nameof(key), $"Max key length is {MAX_KEY_LENGTH}, but encoded key has {key.Length}.");

                var entrySize = 2 + keyBuffer.Length + 8 + 4;
                if (Cursor + entrySize > Range.Max)
                    return false; // index page is full -> fail

                var p = Cursor.Value;
                m_accessor.Write(p, (short)keyBuffer.Length); p += 2;
                m_accessor.WriteArray(p, keyBuffer, 0, keyBuffer.Length); p += keyBuffer.Length;
                m_accessor.Write(p, value.offset); p += 8;
                m_accessor.Write(p, value.size); p += 4;
                Cursor = new(p);
                Count++;

                return true;
            }

            public Guid Magic
            {
                get
                {
                    var buffer = FIELD_MAGIC.Read(m_accessor, Range, 16);
                    return new Guid(buffer);
                }
                private set
                {
                    var buffer = value.ToByteArray();
                    if (buffer.Length != 16) throw new Exception();
                    FIELD_MAGIC.Write(m_accessor, Range, buffer);
                }
            }
            public IndexPage Next
            {
                get
                {
                    var p = FIELD_NEXT.Read(m_accessor, Range);
                    if (p == 0L) return null;
                    return new IndexPage(m_accessor, new Block(new(p), Range.Size));
                }
                private set => FIELD_NEXT.Write(m_accessor, Range, value.Range.Min);
            }
            public Position Cursor
            {
                get => Range + new Offset32(FIELD_CURSOR.Read(m_accessor, Range));
                private set
                {
                    if (value.Value < Range.Min.Value || value.Value > Range.Max.Value) Debugger.Break();
                    var p = (int)(value.Value - Range.Min.Value);
                    FIELD_CURSOR.Write(m_accessor, Range, p);
                }
            }
            public int Count
            {
                get => FIELD_COUNT.Read(m_accessor, Range);
                private set => FIELD_COUNT.Write(m_accessor, Range, value);
            }

            public IEnumerable<(string key, (long, int) value)> Entries
            {
                get
                {
                    //Console.WriteLine($"[IndexPage] {Range}, Next = {Next?.Range.Min}");
                    var p = (Range + FIELD_DATA).Value;
                    var end = Cursor.Value;
                    while (p < end)
                    {
                        var keyBufferLength = m_accessor.ReadInt16(p); p += 2;
                        var keyBuffer = new byte[keyBufferLength];
                        m_accessor.ReadArray(p, keyBuffer, 0, keyBufferLength); p += keyBufferLength;
                        var key = Encoding.UTF8.GetString(keyBuffer);
                        var valueOffset = m_accessor.ReadInt64(p); p += 8;
                        var valueSize = m_accessor.ReadInt32(p); p += 4;
                        var result = (key, (valueOffset, valueSize));
                        //Console.WriteLine($"[ENTRY] [{key}] -> [[{valueOffset}], [{valueSize}]]");
                        yield return result;
                    }
                }
            }
        }

        private class Header
        {
            public const int DefaultHeaderSizeInBytes = 1024;
            public const int DefaultIndexPageSizeInBytes = 256 * 1024;
            public static readonly Guid MagicBytesVersion1 = Guid.Parse("ff682f91-ad99-4135-a5d4-15ef97ed7cde");

            private MemoryMappedViewAccessor m_accessor;
            private Position m_offsetHeader;
            private IndexPage m_currentIndexPage;

            // [ 0] 16 bytes
            public Guid MagicBytes
            {
                get { m_accessor.Read(m_offsetHeader, out Guid x); return x; }
                //set => m_accessor.Write(m_offset, ref value);
            }
            // [16] 4 bytes
            public Offset32 HeaderSizeInBytes
            {
                get => new(m_accessor.ReadInt32(m_offsetHeader + 16));
                //set => m_accessor.Write(m_offset + 16, value);
            }
            // [20] 4 bytes
            public Offset32 IndexPageSizeInBytes
            {
                get => new(m_accessor.ReadInt32(m_offsetHeader + 20));
                //set => m_accessor.Write(m_offset + 20, value);
            }
            // [24] 8 bytes
            public long TotalFileSize
            {
                get => m_accessor.ReadInt64(m_offsetHeader + 24);
                set => m_accessor.Write(m_offsetHeader + 24, value);
            }
            // [32] 8 bytes
            public long TotalIndexEntries
            {
                get => m_accessor.ReadInt64(m_offsetHeader + 32);
                set => m_accessor.Write(m_offsetHeader + 32, value);
            }
            // [40] 8 bytes
            public Position DataCursor
            {
                get => new(m_accessor.ReadInt64(m_offsetHeader + 40));
                set => m_accessor.Write(m_offsetHeader + 40, value);
            }
            // [48] 8 bytes
            public Position IndexRootPageOffset
            {
                get => new(m_accessor.ReadInt64(m_offsetHeader + 48));
                set => m_accessor.Write(m_offsetHeader + 48, value);
            }
            // [56] 16 bytes (8 + 8)
            public DateTimeOffset Created
            {
                get => new(m_accessor.ReadInt64(m_offsetHeader + 56), new TimeSpan(m_accessor.ReadInt64(m_offsetHeader + 64)));
                //set { m_accessor.Write(m_offset + 56, Created.Ticks); m_accessor.Write(m_offset + 64, Created.Offset.Ticks); }
            }

            public Header(MemoryMappedViewAccessor accessor)
            {
                RenewAccessor(accessor);
            }

            public void RenewAccessor(MemoryMappedViewAccessor accessor)
            {
                m_accessor = accessor;
                m_offsetHeader = new(m_accessor.ReadInt64(0L));

                var magicBuffer = new byte[16];
                if (m_accessor.ReadArray(m_offsetHeader, magicBuffer, 0, 16) != 16) throw new Exception("Reading header failed.");
                var magic = new Guid(magicBuffer);
                if (magic != MagicBytesVersion1) throw new Exception("Header does not start with magic bytes.");

                m_currentIndexPage = new IndexPage(m_accessor, new Block(IndexRootPageOffset, IndexPageSizeInBytes));
            }

            public void AppendIndexEntry(string key, long offset, int size, Action<long> ensureSpaceFor)
            {
                if (!m_currentIndexPage.TryAdd(key, (offset, size)))
                {
                    // current index page is full -> append new page
                    ensureSpaceFor(DataCursor + IndexPageSizeInBytes);

                    var newPage = IndexPage.Init(m_accessor, new Block(DataCursor, IndexPageSizeInBytes), next: m_currentIndexPage);
                    
                    // write again, there is enough space now
                    if (!newPage.TryAdd(key, (offset, size))) throw new Exception("Failed to write index entry.");

                    m_currentIndexPage = newPage;
                    DataCursor = newPage.Range.Max;
                    IndexRootPageOffset = newPage.Range.Min;
                }
            }

            public IndexPage IndexRootPage => new(m_accessor, new Block(IndexRootPageOffset, IndexPageSizeInBytes));
            public IEnumerable<IndexPage> IndexPages
            {
                get
                {
                    var p = IndexRootPage;
                    while (p != null)
                    {
                        yield return p;
                        p = p.Next;
                    }
                }
            }

            public void ReadIndexIntoMemory(Dictionary<string, (long, int)> index)
            {
                var xs = IndexPages.SelectMany(p => p.Entries);
                foreach (var (key, value) in xs) index[key] = value;
            }

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
                w.Write(IndexPage.MAGIC_VALUE.ToByteArray());
                w.Write(0L);
                w.Write(16 + 8 + 4 + 4);
                w.Write(0);
                w.Write(new byte[DefaultIndexPageSizeInBytes - 8 - 4]);

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
        //private FileStream m_accessorWriteStream;

        private Stats m_stats;

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
                $"used space            : {m_header.DataCursor,20:N0} bytes (including index)"
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
                            //m_accessorWriteStream.Flush();
                        }

                        m_accessor.Dispose();
                        //m_accessorWriteStream.Dispose();
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
            }

            Log(
                $"",
                $"=========================================",
                $"  starting up (version {Global.Version})",
                $"=========================================",
                $""
                );

            m_dbCache = new Dictionary<string, WeakReference<object>>();
            m_dbCacheKeepAlive = new HashSet<object>();

            if (File.Exists(m_indexFilenameObsolete))
            {
                ParseDeprecatedIndexFile(m_indexFilenameObsolete);
                throw new NotImplementedException("Convert index to new format.");
            }
            else
            {
                m_dbIndex = new Dictionary<string, (long, int)>();
            }

            var totalDataFileSizeInBytes = new FileInfo(m_dataFileName).Length;

            var mapName = m_dataFileName.ToMd5Hash().ToString();

            if (m_readOnlySnapshot)
            {
                try
                {
                    m_mmf = MemoryMappedFile.CreateFromFile(m_dataFileName, FileMode.Open, mapName, totalDataFileSizeInBytes, MemoryMappedFileAccess.Read);
                    //m_accessorWriteStream = null;
                }
                catch
                {
                    m_mmf = MemoryMappedFile.OpenExisting(mapName, MemoryMappedFileRights.Read);
                    //m_accessorWriteStream = null;
                }

                m_accessor = m_mmf.CreateViewAccessor(0, totalDataFileSizeInBytes, MemoryMappedFileAccess.Read);
                m_header = new (m_accessor);
            }
            else
            {
                //m_accessorWriteStream = File.Open(m_dataFileName, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                m_mmf = MemoryMappedFile.CreateFromFile(m_dataFileName, FileMode.OpenOrCreate, mapName, totalDataFileSizeInBytes, MemoryMappedFileAccess.ReadWrite);
                //m_mmf = MemoryMappedFile.CreateFromFile(m_accessorWriteStream, mapName, totalDataFileSizeInBytes, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, false);
                m_accessor = m_mmf.CreateViewAccessor(0, totalDataFileSizeInBytes);
                m_header = new (m_accessor);
                Log(
                    $"reserved space        : {m_header.TotalFileSize,20:N0} bytes",
                    $"used space            : {m_header.DataCursor,20:N0} bytes"
                    );
            }

            if (m_dbIndex.Count > 0) throw new Exception("In-memory index should be empty here.");
            m_header.ReadIndexIntoMemory(m_dbIndex);
        }

        #endregion

        #region Memory-mapped file

        private bool m_mmfIsClosedForResize = false;
        public bool SimulateFullDiskOnNextResize { get; set; }

        private void EnsureSpaceFor(long numberOfBytes)
        {
            if (SimulateFullDiskOnNextResize || m_header.DataCursor + numberOfBytes > m_header.TotalFileSize)
            {
                try
                {
                    var newTotalFileSize = m_header.TotalFileSize;

                    while (m_header.DataCursor + numberOfBytes > newTotalFileSize)
                    {
                        if (newTotalFileSize < 1024 * 1024 * 1024)
                        {
                            newTotalFileSize *= 2;
                        }
                        else
                        {
                            newTotalFileSize += 1024 * 1024 * 1024;
                        }
                    }

                    Log($"resize data file to {newTotalFileSize / (1024 * 1024 * 1024.0):0.000} GiB");
                    lock (m_dbDiskLocation)
                    {
                        //var sw = new Stopwatch(); sw.Restart();

                        m_header.TotalFileSize = newTotalFileSize;

                        m_accessor.Dispose();
                        //m_accessorWriteStream.Dispose();
                        m_mmf.Dispose();

                        m_mmfIsClosedForResize = true;
                        ReOpenMemoryMappedFile(newTotalFileSize);

                        //Console.WriteLine($"resized in {sw.Elapsed}");
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
                    //m_accessorWriteStream = File.Open(m_dataFileName, FileMode.Open, FileAccess.Write, FileShare.ReadWrite);

                    //m_accessorWriteStream = File.Open(m_dataFileName, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                    //m_mmf = MemoryMappedFile.CreateFromFile(m_accessorWriteStream, null, newCapacity, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, false);

                    m_accessor = m_mmf.CreateViewAccessor(0, newCapacity);
                    m_header.RenewAccessor(m_accessor);

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
        public long GetUsedBytes() => m_header.DataCursor;

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

            //var buffer = getEncodedValue?.Invoke();
            lock (m_dbDiskLocation)
            {
                EnsureMemoryMappedFileIsOpen();

                // write value buffer to store
                EnsureSpaceFor(numberOfBytes: buffer.Length);
                var valueBufferPos = m_header.DataCursor;

                WriteBytes(valueBufferPos, buffer);
                //m_accessor.WriteArray(valueBufferPos, buffer, 0, buffer.Length);
                //m_accessorWriteStream.Position = valueBufferPos;
                //m_accessorWriteStream.Write(buffer, 0, buffer.Length);


                m_dbIndex[key] = (valueBufferPos, buffer.Length);
                m_header.DataCursor = valueBufferPos + new Offset32(buffer.Length);

                // write index entry to store
                m_header.AppendIndexEntry(key, valueBufferPos, buffer.Length, EnsureSpaceFor);

                LatestKeyAdded = key;
            }
        }
        private unsafe void WriteBytes(long offset, byte[] data)
        {
            byte* ptr = (byte*)0;
            try
            {
                m_accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
                Marshal.Copy(data, 0, new IntPtr(ptr + offset), data.Length);
            }
            finally
            {
                m_accessor.SafeMemoryMappedViewHandle.ReleasePointer();
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
                //return m_header.IndexPages.SelectMany(p => p.Entries).Select(x => x.key).ToArray();
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
