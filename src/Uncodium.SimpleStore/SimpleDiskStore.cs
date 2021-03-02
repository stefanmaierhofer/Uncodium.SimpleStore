/*
   MIT License
   
   Copyright (c) 2014,2015,2016,2017,2018,2019,2020,2021 Stefan Maierhofer.
   
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
        /// <summary>
        /// The default file extension for a SimpleDiskStore is (U)ncodium (D)isk (S)tore.
        /// </summary>
        public const string DefaultFileExtension = ".uds";

        /// <summary>
        /// The maximum number of characters in a key string.
        /// </summary>
        public const int MaxKeyLength = 4096;

        #region Layout and store conversions

        public enum StoreLayout
        {
            /// <summary>
            /// There is no folder or file at the given location.
            /// </summary>
            None,

            /// <summary>
            /// Deprecated. Original layout consisting of a folder containing files 'data.bin' and 'index.bin'.
            /// </summary>
            FolderWithStandaloneDataAndIndexFiles,

            /// <summary>
            /// A folder containing a single 'data.bin' file with integrated index.
            /// This is the result of converting an original (deprecated) store into the current single file format.
            /// </summary>
            FolderWithMergedDataAndIndexFile,

            /// <summary>
            /// The current format. A single file containing data and index.
            /// </summary>
            SingleFile,

            /// <summary>
            /// There is a folder or file, but in an unknown format.
            /// </summary>
            Unknown
        }

        public static StoreLayout GetStoreLayout(string path)
        {
            if (Directory.Exists(path))
            {
                var dataFileName = Path.Combine(path, "data.bin");
                if (File.Exists(dataFileName))
                {
                    var indexFileName = Path.Combine(path, "index.bin");
                    if (File.Exists(indexFileName))
                    {
                        return StoreLayout.FolderWithStandaloneDataAndIndexFiles;
                    }
                    else
                    {
                        return ContainsHeader(dataFileName) ? StoreLayout.FolderWithMergedDataAndIndexFile : StoreLayout.Unknown;
                    }
                }
                else
                {
                    // a 'data.bin' file is required inside a folder layout
                    return StoreLayout.Unknown;
                }
            }
            else
            {
                // if there is no folder, then this must be a single file layout ...
                var fileName = path;
                if (!File.Exists(fileName))
                {
                    fileName += DefaultFileExtension;
                    if (!File.Exists(fileName))
                    {
                        // there is neither a folder nor a file at the given path
                        return StoreLayout.None;
                    }
                }

                return ContainsHeader(fileName) ? StoreLayout.SingleFile : StoreLayout.Unknown;
            }
        }

        private static bool ContainsHeader(string fileName)
        {
            // if file is too small for header magic bytes, then it is not current format
            var dataFileSize = new FileInfo(fileName).Length;
            if (dataFileSize < 16) return false;

            using var br = new BinaryReader(File.Open(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            // check if file starts with header ...
            if (new Guid(br.ReadBytes(16)) == Header.MagicBytesVersion1)
            {
                // data file starts with header -> must be current format
                return true;
            }
            else
            {
                // first 8 bytes may also be offset to header
                // (if data file has previously been converted from old format)
                br.BaseStream.Seek(0L, SeekOrigin.Begin);
                var p = br.ReadInt64();

                // check if offset p is within file ...
                if (p < 0 || p > dataFileSize - 16)
                {
                    // invalid offset -> can't be current format
                    return false;
                }

                // check if header starts at offset p ...
                br.BaseStream.Seek(p, SeekOrigin.Begin);
                if (new Guid(br.ReadBytes(16)) == Header.MagicBytesVersion1)
                {
                    // header found -> must be current format
                    return true;
                }
                else
                {
                    // no header at offset p -> can't be current format
                    return false;
                }
            }
        }

        private void UpgradeOriginalStore(string folder)
        {
            if (!Directory.Exists(folder)) throw new Exception($"Expected folder {folder}.");

            var indexFileName = Path.GetFullPath(Path.Combine(folder, "index.bin"));
            if (!File.Exists(indexFileName)) throw new Exception($"Expected {indexFileName}.");

            var dataFilename = Path.GetFullPath(Path.Combine(folder, "data.bin")); 
            if (!File.Exists(dataFilename)) throw new Exception($"Expected {dataFilename}.");

            //
            Log($"Upgrading store to format {Header.MagicBytesVersion1}.");

            // (1) inject a header and empty index into old-style data file ...
            var totalDataFileSizeInBytes = InjectHeaderAndEmptyIndex(dataFilename);

            // (2) import deprecated index ...
            if (m_mmf != null) throw new Exception("Invariant ec56f623-bc1d-41cf-9421-3d4e5cacb9ce.");
            if (m_accessor != null) throw new Exception("Invariant caed99a0-55ad-433f-baeb-e0db9fc139c7.");
            if (m_header != null) throw new Exception("Invariant 55926fae-4371-498c-b365-5e451c976018.");
            if (m_dbIndex != null) throw new Exception("Invariant e0e827e5-c038-4d0e-8e55-9a235ad4f353.");
            if (m_dataFileName != null) throw new Exception("Invariant a7038834-41c4-41ac-8b55-016f2f9d2969.");

            m_dataFileName = dataFilename;
            var mapName = m_dataFileName.ToMd5Hash().ToString();
            m_mmf = MemoryMappedFile.CreateFromFile(m_dataFileName, FileMode.OpenOrCreate, mapName, totalDataFileSizeInBytes, MemoryMappedFileAccess.ReadWrite);
            m_accessor = m_mmf.CreateViewAccessor(0, totalDataFileSizeInBytes);
            m_header = new(m_accessor);
            m_dbIndex = new Dictionary<string, IndexEntry>();

            ImportDeprecatedIndexFile(indexFileName);
            Flush();

            m_dbIndex = null;
            m_header = null;
            m_accessor.Dispose(); m_accessor = null;
            m_mmf.Dispose(); m_mmf = null;
            m_dataFileName = null;

            // (3) delete old index file ...
            Log($"deleting deprecated index file: {indexFileName}");
            File.Delete(indexFileName);

            Log($"Sucessfully upgraded store to format {Header.MagicBytesVersion1}.");

            // returns total data file size
            long InjectHeaderAndEmptyIndex(string filename)
            {
                if (ContainsHeader(filename)) throw new Exception(
                    $"Failed to upgrade original store format. Data file already contains header format {Header.MagicBytesVersion1}."
                    );

                var totalDataFileSizeInBytes = new FileInfo(filename).Length;
                Log($"    total data file size                          : {indexFileName,20:N0} bytes");

                // get write position (first 8 bytes as int64, in old format)
                var p = 0L;
                using (var br = new BinaryReader(File.Open(filename, FileMode.Open, FileAccess.Read, FileShare.None)))
                    p = br.ReadInt64();
                Log($"    write position                                : {p,20:N0}");

                // append header and empty index (at current write position)
                using var f = File.Open(filename, FileMode.Open, FileAccess.Write, FileShare.None);

                var headerOffset = p;
                f.Position = headerOffset;
                var buffer = Header.GenerateHeaderAndEmptyIndexForOffset(headerOffset, totalDataFileSizeInBytes);
                f.Write(buffer, 0, buffer.Length);
                var writePosition = f.Position;
                Log($"    injected header and empty index");
                Log($"    write position (new)                          : {writePosition,20:N0}");

                // replace write position with pointer/offset to header
                var pBuffer = BitConverter.GetBytes(headerOffset);
                f.Position = 0L;
                f.Write(pBuffer, 0, 8);
                Log($"    replaced write position with header offset    : {headerOffset,20:N0}");
                var totalDataFileSizeInBytesNew = Math.Max(writePosition, totalDataFileSizeInBytes);
                Log($"    total data file size (new)                    : {totalDataFileSizeInBytesNew,20:N0}");
                return totalDataFileSizeInBytesNew;
            }

            void ImportDeprecatedIndexFile(string filename)
            {
                Log($"importing deprecated index file: {filename}");
                using var f = File.Open(filename, FileMode.Open, FileAccess.Read, FileShare.Read);
                using var br = new BinaryReader(f);
                var count = 0;
                var i = 0;
                var key = string.Empty;
                var offset = 0UL;
                var size = 0U;
                try
                {
                    // import existing (deprecated) index file
                    count = br.ReadInt32();
                    Log($"entries: {count:N0}");
                    var sw = new Stopwatch(); sw.Start();
                    for (i = 0; i < count; i++)
                    {
                        key = br.ReadString();
                        offset = br.ReadUInt64();
                        size = br.ReadUInt32();

                        var e = new IndexEntry(key, offset, size);
                        m_header.AppendIndexEntry(e, EnsureSpaceFor);
                    }
                    sw.Stop();
                    Log(
                        $"imported deprecated index file in {sw.Elapsed}",
                        $"that's appr. {(int)(count / sw.Elapsed.TotalSeconds):N0} entries/second"
                        );
                }
                catch (Exception e)
                {
                    Log(
                        $"[CRITICAL ERROR] Damaged index file {filename}",
                        $"[CRITICAL ERROR] Error a7814485-0e86-422e-92f0-9a4a31216a27.",
                        $"[CRITICAL ERROR] Could read {i:N0}/{count:N0} index entries.",
                        $"[CRITICAL ERROR] Last entry: {key} @ +{offset:N0} with size {size:N0} bytes.",
                        $"[CRITICAL ERROR] {e}"
                    );
                }
            }
        }

        #endregion

        #region Private

        private class IndexEntry
        {
            public readonly string Key;
            public readonly ulong Offset;
            public readonly uint Size;

            public IndexEntry(string key, ulong offset, uint size)
            {
                if (key.Length > MaxKeyLength)
                    throw new ArgumentOutOfRangeException(nameof(key), $"Max key length is {MaxKeyLength}, but key has {key.Length}.");

                Key = key;
                Offset = offset;
                Size = size;
            }
        }

        private struct Position
        {
            public static readonly Position Zero = new(0L);

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
            public static readonly Guid MagicBytesVersion1 = Guid.Parse("5587b343-403d-4f62-af22-a18da686b1da");
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
                    Magic = MagicBytesVersion1,
                    Next = next,
                    Cursor = cursor,
                    Count = 0
                };

                if (result.Cursor.Value < result.Range.Min.Value || result.Cursor.Value > result.Range.Max.Value) Debugger.Break();

                return result;
            }

            public bool TryAdd(IndexEntry e)
            {
                var keyBuffer = Encoding.UTF8.GetBytes(e.Key);
                if (keyBuffer.Length > MaxKeyLength)
                    throw new ArgumentOutOfRangeException(nameof(e.Key), $"Max key length is {MaxKeyLength}, but encoded key has {e.Key.Length}.");

                var entrySize = 2 + keyBuffer.Length + 8 + 4 + 4;
                if (Cursor + entrySize > Range.Max)
                    return false; // index page is full -> fail

                var p = Cursor.Value;
                m_accessor.Write(p, (short)keyBuffer.Length); p += 2;
                m_accessor.WriteArray(p, keyBuffer, 0, keyBuffer.Length); p += keyBuffer.Length;
                m_accessor.Write(p, e.Offset); p += 8;
                m_accessor.Write(p, e.Size); p += 4;
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
                private set => FIELD_NEXT.Write(m_accessor, Range, value != null ? value.Range.Min : Position.Zero);
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

            public IEnumerable<(string key, IndexEntry value)> Entries
            {
                get
                {
                    //Console.WriteLine($"[IndexPage] {Range}, Next = {Next?.Range.Min}");
                    var p = (Range + FIELD_DATA).Value;
                    var end = Cursor.Value;
                    while (p < end)
                    {
                        var keyBufferLength = m_accessor.ReadUInt16(p); p += 2;
                        var keyBuffer = new byte[keyBufferLength];
                        m_accessor.ReadArray(p, keyBuffer, 0, keyBufferLength); p += keyBufferLength;
                        var key = Encoding.UTF8.GetString(keyBuffer);
                        var valueOffset = m_accessor.ReadUInt64(p); p += 8;
                        var valueSize = m_accessor.ReadUInt32(p); p += 4;
                        var result = (key, new IndexEntry(key, valueOffset, valueSize));
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

            #region Properties

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

            #endregion

            public Header(MemoryMappedViewAccessor accessor)
            {
                RenewAccessor(accessor);
            }

            public void RenewAccessor(MemoryMappedViewAccessor accessor)
            {
                m_accessor = accessor;

                // check if data file immediately starts with header ...
                if (IsHeaderAtOffset(0L))
                {
                    m_offsetHeader = new(0L);
                }
                else
                {
                    // ... otherwise first 8 bytes may be offset to real header ...
                    var offset = m_accessor.ReadInt64(0L);
                    if (IsHeaderAtOffset(offset))
                    {
                        m_offsetHeader = new(offset);
                    }
                    else
                    {
                        throw new Exception("Can't find header.");
                    }
                }

                m_currentIndexPage = new IndexPage(m_accessor, new Block(IndexRootPageOffset, IndexPageSizeInBytes));

                bool IsHeaderAtOffset(long p)
                {
                    var magicBuffer = new byte[16];
                    if (m_accessor.ReadArray(p, magicBuffer, 0, 16) != 16) throw new Exception("Reading header failed.");
                    var magic = new Guid(magicBuffer);
                    return magic == MagicBytesVersion1;
                }
            }

            public void AppendIndexEntry(IndexEntry e, Action<long> ensureSpaceFor)
            {
                if (!m_currentIndexPage.TryAdd(e))
                {
                    // current index page is full -> append new page
                    ensureSpaceFor(DataCursor + IndexPageSizeInBytes);

                    var newPage = IndexPage.Init(m_accessor, new Block(DataCursor, IndexPageSizeInBytes), next: m_currentIndexPage);
                    
                    // write again, there is enough space now
                    if (!newPage.TryAdd(e)) throw new Exception("Failed to write index entry.");

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

            public void ReadIndexIntoMemory(Dictionary<string, IndexEntry> index)
            {
                var xs = IndexPages.SelectMany(p => p.Entries);
                foreach (var (key, value) in xs) index[key] = value;
            }

            public static void CreateEmptyDataFile(string dataFileName)
            {
                using var f = File.Open(dataFileName, FileMode.CreateNew, FileAccess.Write, FileShare.None);
                using var w = new BinaryWriter(f);

                var buffer = GenerateHeaderAndEmptyIndexForOffset(0L, 0L);
                w.Write(buffer);
                w.Flush();
            }

            public static byte[] GenerateHeaderAndEmptyIndexForOffset(long offset, long totalFileSizeInBytes)
            {
                var ms = new MemoryStream();
                using var w = new BinaryWriter(ms);

                var indexRootPageOffset = offset + DefaultHeaderSizeInBytes;
                var cursorPos           = offset + DefaultHeaderSizeInBytes + DefaultIndexPageSizeInBytes;
                var totalFileSize       = Math.Max(cursorPos, totalFileSizeInBytes);
                var totalIndexEntries   = 0L;

                w.Write(MagicBytesVersion1.ToByteArray());              // [ 0] MagicBytesVersion1
                w.Write(DefaultHeaderSizeInBytes);                      // [16] HeaderSizeInBytes
                w.Write(DefaultIndexPageSizeInBytes);                   // [20] IndexPageSizeInBytes
                w.Write(totalFileSize);                                 // [24] TotalFileSize
                w.Write(totalIndexEntries);                             // [32] TotalIndexEntries
                w.Write(cursorPos);                                     // [40] CursorPos
                w.Write(indexRootPageOffset);                           // [48] IndexPos
                w.Write(DateTimeOffset.Now.Ticks);                      // [56] Created.Ticks
                w.Write(DateTimeOffset.Now.Offset.Ticks);               //             .Offset

                w.BaseStream.Position = DefaultHeaderSizeInBytes;
                w.Write(IndexPage.MagicBytesVersion1.ToByteArray());    // [ 0] magic bytes
                w.Write(0L);                                            // [16] next
                w.Write(16 + 8 + 4 + 4);                                // [24] cursor
                w.Write(0);                                             // [28] count
                w.Write(new byte[DefaultIndexPageSizeInBytes - 8 - 4]); // [32] data

                w.Flush();

                return ms.ToArray();
            }
        }

        private readonly object m_lock = new();
        private readonly string m_dbDiskLocation;
        private readonly bool m_readOnlySnapshot;
        private string m_dataFileName;

        private Dictionary<string, IndexEntry> m_dbIndex;
        private Dictionary<string, WeakReference<object>> m_dbCache;
        private HashSet<object> m_dbCacheKeepAlive;
        private Header m_header;
        private MemoryMappedFile m_mmf;
        private MemoryMappedViewAccessor m_accessor;

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
                            $"shutdown {token} - latest known key is {Stats.LatestKeyAdded},",
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
        /// Creates store in given file.
        /// Optionally opens current state read-only.
        /// Optionally a logger can be supplied which replaces the default logger to log.txt.
        /// </summary>
        private SimpleDiskStore(string path, bool readOnlySnapshot, Action<string[]> logLines)
        {
            m_dbDiskLocation = path;
            m_readOnlySnapshot = readOnlySnapshot;

            if (logLines != null)
            {
                f_logLines = logLines;
            }
            else
            {
                string logFileName;
                if (Directory.Exists(m_dbDiskLocation))
                {
                    // deprecated format/layout
                    logFileName = Path.Combine(m_dbDiskLocation, "log.txt");
                }
                else
                {
                    // new format/layout
                    logFileName = m_dbDiskLocation.EndsWith(DefaultFileExtension)
                        ? m_dbDiskLocation + ".log"
                        : m_dbDiskLocation + DefaultFileExtension + ".log";
                }
                f_logLines = lines => File.AppendAllLines(logFileName, lines.Select(line => $"[{DateTimeOffset.Now}] {line}"));
            }

            Init();
        }

        /// <summary>
        /// Creates store in given file.
        /// Optionally opens current state read-only.
        /// </summary>
        private SimpleDiskStore(string path, bool readOnlySnapshot)
            : this(path, readOnlySnapshot: readOnlySnapshot, logLines: null)
        { }

        /// <summary>
        /// Creates store in given file.
        /// </summary>
        public SimpleDiskStore(string path) 
            : this(path, readOnlySnapshot: false, logLines: null)
        { }

        /// <summary>
        /// Creates store in given file.
        /// Optionally a logger can be supplied which replaces the default logger to log.txt.
        /// </summary>
        public SimpleDiskStore(string path, Action<string[]> logLines)
            : this(path, readOnlySnapshot: false, logLines: logLines)
        { }

        /// <summary>
        /// Opens store in given file in read-only snapshot mode.
        /// This means that no store entries that are added after the call to OpenReadOnlySnapshot will be(come) visible.
        /// </summary>
        public static SimpleDiskStore OpenReadOnlySnapshot(string path)
            => new(path, readOnlySnapshot: true);

        #endregion

        private void Init()
        {
            // === AUTO-CONVERT deprecated formats ===
            var layout = GetStoreLayout(m_dbDiskLocation);

            if (layout == StoreLayout.FolderWithStandaloneDataAndIndexFiles)
            {
                // FolderWithStandaloneDataAndIndexFiles --> FolderWithMergedDataAndIndexFile
                UpgradeOriginalStore(m_dbDiskLocation);
                layout = GetStoreLayout(m_dbDiskLocation);
            }

            // === INIT ===

            // init data file
            switch (layout)
            {
                case StoreLayout.Unknown:
                    throw new Exception($"Unknown store layout at '{m_dbDiskLocation}'.");

                case StoreLayout.FolderWithStandaloneDataAndIndexFiles:
                    // never reached (has been auto-converted to FolderWithmergedDataAndIndexFile above)
                    throw new Exception($"Invariant 98a8fe60-a87d-4c16-a47e-9b760d09788b.");

                case StoreLayout.FolderWithMergedDataAndIndexFile:
                    {
                        m_dataFileName = Path.Combine(m_dbDiskLocation, "data.bin");
                        if (!File.Exists(m_dataFileName)) throw new Exception($"Data file '{m_dataFileName}' does not exist.");
                    }
                    break;

                case StoreLayout.SingleFile:
                    {
                        m_dataFileName = m_dbDiskLocation;
                        if (!File.Exists(m_dataFileName) && !m_dataFileName.EndsWith(DefaultFileExtension))
                        {
                            m_dataFileName += DefaultFileExtension;
                            if (!File.Exists(m_dataFileName)) throw new Exception($"Store file '{m_dataFileName}' does not exist.");
                        }
                    }
                    break;

                case StoreLayout.None:
                    {
                        m_dataFileName = m_dbDiskLocation;
                        if (!m_dataFileName.EndsWith(DefaultFileExtension)) m_dataFileName += DefaultFileExtension;

                        // create empty store
                        Header.CreateEmptyDataFile(m_dataFileName);
                    }
                    break;

                default:
                    throw new Exception($"Unknown layout '{layout}'.");
            }
            
            // init
            Log(
                $"",
                $"=========================================",
                $"  starting up (version {Global.Version})",
                $"=========================================",
                $""
                );

            m_dbCache = new Dictionary<string, WeakReference<object>>();
            m_dbCacheKeepAlive = new HashSet<object>();

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
            }
            else
            {
                m_mmf = MemoryMappedFile.CreateFromFile(m_dataFileName, FileMode.OpenOrCreate, mapName, totalDataFileSizeInBytes, MemoryMappedFileAccess.ReadWrite);
                m_accessor = m_mmf.CreateViewAccessor(0, totalDataFileSizeInBytes);
                
            }

            // init header
            m_header = new(m_accessor);
            Log(
                $"reserved space        : {m_header.TotalFileSize,20:N0} bytes",
                $"used space            : {m_header.DataCursor,20:N0} bytes"
                );

            // create in-memory index
            m_dbIndex = new Dictionary<string, IndexEntry>();
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
                    lock (m_lock)
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
            lock (m_lock)
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
        public Stats Stats => m_stats.Copy();

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
        /// </summary>
        public void Add(string key, byte[] value)
        {
            CheckDisposed();

            if (m_readOnlySnapshot) throw new InvalidOperationException("Read-only store does not support add.");
            if (value == null) throw new ArgumentNullException(nameof(value));

            Interlocked.Increment(ref m_stats.CountAdd);

            var buffer = value;

            lock (m_lock)
            {
                EnsureMemoryMappedFileIsOpen();

                var valueBufferPos = m_header.DataCursor;
                var storedLength = buffer.Length;

                // write value buffer to store
                EnsureSpaceFor(numberOfBytes: buffer.Length);
                WriteBytes(valueBufferPos, buffer, 0, buffer.Length);
                m_header.DataCursor = valueBufferPos + new Offset32(buffer.Length);

                // update index
                var e = new IndexEntry(key, (ulong)valueBufferPos.Value, (uint)storedLength);
                m_dbIndex[key] = e;
                m_header.AppendIndexEntry(e, EnsureSpaceFor);

                // housekeeping
                m_stats.LatestKeyAdded = key;
            }
        }
        
        private unsafe void WriteBytes(long writeOffset, byte[] data, int dataOffset, int dataLength)
        {
            byte* ptr = (byte*)0;
            try
            {
                m_accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
                Marshal.Copy(data, dataOffset, new IntPtr(ptr + writeOffset), dataLength);
            }
            finally
            {
                m_accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            }
        }

        private unsafe void WriteBytes(long writeOffset, ReadOnlySpan<byte> data)
        {
            byte* ptr = (byte*)0;
            try
            {
                m_accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
                var destination = new Span<byte>(ptr + writeOffset, data.Length);
                data.CopyTo(destination);
            }
            finally
            {
                m_accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            }
        }

        public void AddStream(string key, Stream stream)
        {
            CheckDisposed();

            if (m_readOnlySnapshot) throw new InvalidOperationException("Read-only store does not support add.");

            using var ms = new MemoryStream();
            stream.CopyTo(ms);
            var buffer = ms.ToArray();

            Interlocked.Increment(ref m_stats.CountAdd);

            lock (m_lock)
            {
                EnsureMemoryMappedFileIsOpen();

                var valueBufferPos = m_header.DataCursor;
                var storedLength = buffer.Length;

                // write value buffer to store
                EnsureSpaceFor(numberOfBytes: buffer.Length);
                WriteBytes(valueBufferPos, buffer, 0, buffer.Length);
                m_header.DataCursor = valueBufferPos + new Offset32(buffer.Length);

                // update index
                var e = new IndexEntry(key, (ulong)valueBufferPos.Value, (uint)storedLength);
                m_dbIndex[key] = e;
                m_header.AppendIndexEntry(e, EnsureSpaceFor);

                // housekeeping
                m_stats.LatestKeyAdded = key;
            }
        }

        /// <summary>
        /// True if key is contained in store.
        /// </summary>
        public bool Contains(string key)
        {
            CheckDisposed();

            bool result;
            lock (m_lock)
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

            lock (m_lock)
            {
                EnsureMemoryMappedFileIsOpen();
                if (m_dbIndex.TryGetValue(key, out IndexEntry entry))
                {
                    try
                    {
                        var buffer = new byte[entry.Size];
                        var offset = (long)entry.Offset;
                        if (offset < 0) throw new Exception($"Offset out of range. Should not be greater than {long.MaxValue}, but is {entry.Offset}.");
                        var readcount = m_accessor.ReadArray(offset, buffer, 0, buffer.Length);
                        if (readcount != buffer.Length) throw new InvalidOperationException();
                        Interlocked.Increment(ref m_stats.CountGet);
                        return buffer;
                    }
                    catch (Exception e)
                    {
                        var count = Interlocked.Increment(ref m_stats.CountGetWithException);
                        Log($"[CRITICAL ERROR] Get(key={key}) failed.",
                            $"[CRITICAL ERROR] entry = {{offset={entry.Offset}, size={entry.Size}}}",
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

            lock (m_lock)
            {
                EnsureMemoryMappedFileIsOpen();
                if (m_dbIndex.TryGetValue(key, out IndexEntry entry))
                {
                    if (offset >= entry.Size) throw new ArgumentOutOfRangeException(nameof(offset), "Offset must be less than length of value buffer.");
                    if (offset + length > entry.Size) throw new ArgumentOutOfRangeException(nameof(offset), "Offset + size exceeds length of value buffer.");

                    try
                    {
                        var buffer = new byte[length];
                        var o = entry.Offset + (ulong)offset;
                        if (o > long.MaxValue) throw new Exception($"Offset out of range. Should not be greater than {long.MaxValue}, but is {o}.");
                        if (length > int.MaxValue) throw new Exception($"Length out of range. Should not be greater than {int.MaxValue}, but is {length}.");
                        var readcount = m_accessor.ReadArray((long)o, buffer, 0, (int)length);
                        if (readcount != length) throw new InvalidOperationException();
                        Interlocked.Increment(ref m_stats.CountGetSlice);
                        return buffer;
                    }
                    catch (Exception e)
                    {
                        var count = Interlocked.Increment(ref m_stats.CountGetSliceWithException);
                        Log($"[CRITICAL ERROR] GetSlice(key={key}, offset={offset}, length={length}) failed.",
                            $"[CRITICAL ERROR] entry = {{offset={entry.Offset}, size={entry.Size}}}",
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
        /// Get read stream for data with given key.
        /// This is not thread-safe with respect to overwriting or removing existing values.
        /// </summary>
        /// <param name="key">Retrieve data for this key.</param>
        /// <param name="offset">Optional. Start stream at given position.</param>
        public Stream GetStream(string key, long offset = 0L)
        {
            CheckDisposed();

            lock (m_lock)
            {
                EnsureMemoryMappedFileIsOpen();
                if (m_dbIndex.TryGetValue(key, out IndexEntry entry))
                {
                    if (offset < 0L || offset >= entry.Size) throw new ArgumentOutOfRangeException(
                        nameof(offset), $"Offset {offset:N0} is out of valid range [0, {entry.Size:N0})."
                        );

                    Interlocked.Increment(ref m_stats.CountGetStream);
                    return m_mmf.CreateViewStream((long)entry.Offset + offset, entry.Size, MemoryMappedFileAccess.Read);
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
            lock (m_lock)
            {
                EnsureMemoryMappedFileIsOpen();
                m_dbIndex.Remove(key);
            }
            Interlocked.Increment(ref m_stats.CountRemove);
        }

        /// <summary>
        /// Enumerate all entries.
        /// </summary>
        public IEnumerable<(string key, long size)> List()
        {
            CheckDisposed();

            lock (m_lock)
            {
                Interlocked.Increment(ref m_stats.CountList);
                return m_dbIndex.Select(x =>(key: x.Key, size: (long)x.Value.Size) );
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
                lock (m_lock)
                {
                    EnsureMemoryMappedFileIsOpen();
                    m_accessor.Flush();
                }
            }
        }
    }
}
