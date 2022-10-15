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

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Uncodium.SimpleStore;

/// <summary>
/// Simple folder store with one file per entry.
/// </summary>
public class SimpleFolderStore : ISimpleStore, ISimpleStoreAsync
{
    /// <summary>
    /// The store folder.
    /// </summary>
    public string Folder { get; }

    private string GetFileNameFromId(string id) => Path.Combine(Folder, id);
    private Stats m_stats;

    private bool m_isDisposed = false;
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CheckDisposed() { if (m_isDisposed) throw new ObjectDisposedException(nameof(SimpleFolderStore)); }

    /// <summary>
    /// Creates a store in the given folder.
    /// </summary>
    public SimpleFolderStore(string folder)
    {
        Folder = folder;
        if (!Directory.Exists(Folder)) Directory.CreateDirectory(folder);
    }

    public void Dispose()
    {
        CheckDisposed();
        m_isDisposed = true;
    }

    #region ISimpleStore

    /// <summary>
    /// </summary>
    public bool IsDisposed => m_isDisposed;

    /// <summary>
    /// Add data from buffer.
    /// </summary>
    public void Add(string key, byte[] value)
    {
        CheckDisposed();

        var dir = Path.GetDirectoryName(key);
        if (dir != "" && !Directory.Exists(dir)) Directory.CreateDirectory(GetFileNameFromId(dir));
        var filename = GetFileNameFromId(key);

        File.WriteAllBytes(filename, value);

        Interlocked.Increment(ref m_stats.CountAdd);
        m_stats.LatestKeyAdded = m_stats.LatestKeyFlushed = key;
    }

    /// <summary>
    /// Add data from stream.
    /// </summary>
    public void AddStream(string key, Stream stream, Action<long>? onProgress = default, CancellationToken ct = default)
    {
        CheckDisposed();

        var dir = Path.GetDirectoryName(key);
        if (dir != "" && !Directory.Exists(dir)) Directory.CreateDirectory(GetFileNameFromId(dir));
        var filename = GetFileNameFromId(key);

        using var target = File.Open(filename, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);

        ct.ThrowIfCancellationRequested();
        target.Position = 0L;
        //stream.CopyTo(target);

        Task.Run(async () =>
        {
            var buffer = new byte[81920];
            int bytesRead;
            long totalRead = 0;
            while ((bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length, ct)) > 0)
            {
                await target.WriteAsync(buffer, 0, bytesRead, ct);
                ct.ThrowIfCancellationRequested();
                totalRead += bytesRead;
                if (onProgress != default) onProgress(totalRead);
            }
        }, ct).Wait();

        Interlocked.Increment(ref m_stats.CountAdd);
        m_stats.LatestKeyAdded = m_stats.LatestKeyFlushed = key;
    }

    public Stream GetWriteStream(string key, bool overwrite = true, Action<long>? onProgress = null, CancellationToken ct = default)
    {
        CheckDisposed();

        var dir = Path.GetDirectoryName(key);
        if (dir != "" && !Directory.Exists(dir)) Directory.CreateDirectory(GetFileNameFromId(dir));
        var filename = GetFileNameFromId(key);

        var target = File.Open(filename, overwrite ? FileMode.Create : FileMode.CreateNew, FileAccess.Write, FileShare.None);

        ct.ThrowIfCancellationRequested();

        Interlocked.Increment(ref m_stats.CountAdd);
        m_stats.LatestKeyAdded = m_stats.LatestKeyFlushed = key;

        return target;
    }

    /// <summary>
    /// True if key exists in store.
    /// </summary>
    public bool Contains(string key)
    {
        CheckDisposed();
        Interlocked.Increment(ref m_stats.CountContains);
        return File.Exists(GetFileNameFromId(key));
    }

    /// <summary>
    /// Get size of value in bytes,
    /// or null if key does not exist.
    /// </summary>
    public long? GetSize(string key)
    {
        CheckDisposed();

        var filename = GetFileNameFromId(key);

        // we intentionally do not handle the case where a file is deleted between 'exists' and 'fileinfo',
        // in order to let the caller know that there is a race condition in the calling code
        return File.Exists(filename) ? new FileInfo(filename).Length : null;
    }

    /// <summary>
    /// Get value as buffer,
    /// or null if key does not exist.
    /// </summary>
    public byte[]? Get(string key)
    {
        CheckDisposed();

        Interlocked.Increment(ref m_stats.CountGet);
        try
        {
            var buffer = File.ReadAllBytes(GetFileNameFromId(key));
            Interlocked.Increment(ref m_stats.CountGet);
            return buffer;
        }
        catch
        {
            Interlocked.Increment(ref m_stats.CountGetInvalidKey);
            return null;
        }
    }

    /// <summary>
    /// Get value slice as buffer,
    /// or null if key does not exist.
    /// </summary>
    public byte[]? GetSlice(string key, long offset, int size)
    {
        CheckDisposed();

        if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), "Offset must be greater or equal 0.");
        if (size < 1) throw new ArgumentOutOfRangeException(nameof(offset), "Size must be greater than 0.");

        Interlocked.Increment(ref m_stats.CountGetSlice);
        try
        {
            using var fs = File.Open(GetFileNameFromId(key), FileMode.Open, FileAccess.Read, FileShare.Read);
            fs.Seek(offset, SeekOrigin.Begin);
            using var br = new BinaryReader(fs);
            var buffer = br.ReadBytes(size);
            Interlocked.Increment(ref m_stats.CountGet);
            return buffer;
        }
        catch
        {
            Interlocked.Increment(ref m_stats.CountGetInvalidKey);
            return null;
        }
    }

    /// <summary>
    /// Get value as read stream,
    /// or null if key does not exist.
    /// This is not thread-safe with respect to overwriting or removing existing values.
    /// </summary>
    /// <param name="key">Retrieve data for this key.</param>
    /// <param name="offset">Optional. Start stream at given position.</param>
    public Stream? GetStream(string key, long offset = 0L)
    {
        CheckDisposed();

        Interlocked.Increment(ref m_stats.CountGetStream);
        try
        {
            var stream = File.Open(GetFileNameFromId(key), FileMode.Open, FileAccess.Read, FileShare.Read);
            stream.Position = offset;
            Interlocked.Increment(ref m_stats.CountGetStream);
            return stream;
        }
        catch
        {
            Interlocked.Increment(ref m_stats.CountGetInvalidKey);
            return null;
        }
    }

    /// <summary>
    /// Enumerate all entries as (key, size) tuples.
    /// </summary>
    public IEnumerable<(string key, long size)> List()
    {
        CheckDisposed();
        var skip = Folder.Length + 1;
        return Directory
            .EnumerateFiles(Folder, "*.*", SearchOption.AllDirectories)
            .Select(x => (key: x.Substring(skip), size: new FileInfo(x).Length))
            ;
    }

    /// <summary>
    /// Remove entry.
    /// </summary>
    public void Remove(string key)
    {
        CheckDisposed();

        try
        {
            File.Delete(GetFileNameFromId(key));

            var dir = Path.GetDirectoryName(key);
            while (dir != "" && !Directory.EnumerateFileSystemEntries(GetFileNameFromId(dir)).Any())
            {
                Directory.Delete(GetFileNameFromId(dir));
                dir = Path.GetDirectoryName(dir);
            }

            Interlocked.Increment(ref m_stats.CountRemove);
        }
        catch
        {
            Interlocked.Increment(ref m_stats.CountRemoveInvalidKey);
        }
    }

    /// <summary>
    /// Commit any pending changes to backing storage.
    /// </summary>
    public void Flush()
    {
        CheckDisposed();
        Interlocked.Increment(ref m_stats.CountFlush);
    }

    /// <summary>
    /// Total bytes used for data.
    /// </summary>
    public long GetUsedBytes()
        => Directory.EnumerateFiles(Folder).Select(s => new FileInfo(s).Length).Sum();

    /// <summary>
    /// Total bytes reserved for data.
    /// </summary>
    public long GetReservedBytes() => GetUsedBytes();

    /// <summary>
    /// Current version.
    /// </summary>
    public string Version => Global.Version;

    /// <summary>
    /// Various runtime counts and other statistics.
    /// </summary>
    public Stats Stats => m_stats.Copy();

    #endregion

    #region ISimpleStoreAsync

    public async Task AddAsync(string key, byte[] data, Action<long>? onProgress = null, CancellationToken ct = default)
    {
        CheckDisposed();

        var dir = Path.GetDirectoryName(key);
        if (dir != "" && !Directory.Exists(dir)) Directory.CreateDirectory(GetFileNameFromId(dir));
        var filename = GetFileNameFromId(key);

        using var f = File.Open(filename, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);
        await f.WriteAsync(data, 0, data.Length, ct);
        onProgress?.Invoke(data.Length);

        Interlocked.Increment(ref m_stats.CountAdd);
        m_stats.LatestKeyAdded = m_stats.LatestKeyFlushed = key;
    }

    public async Task AddStreamAsync(string key, Stream data, Action<long>? onProgress = null, CancellationToken ct = default)
    {
        CheckDisposed();

        var dir = Path.GetDirectoryName(key);
        if (dir != "" && !Directory.Exists(dir)) Directory.CreateDirectory(GetFileNameFromId(dir));
        var filename = GetFileNameFromId(key);

        using var target = File.Open(filename, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);
        var startPosition = data.Position;
        await data.CopyToAsync(target, 81920, ct);
        onProgress?.Invoke(data.Position - startPosition);

        Interlocked.Increment(ref m_stats.CountAdd);
        m_stats.LatestKeyAdded = m_stats.LatestKeyFlushed = key;
    }

    public Task<bool> ContainsAsync(string key, CancellationToken ct = default)
        => Task.FromResult(Contains(key));

    public Task<long?> GetSizeAsync(string key, CancellationToken ct = default)
        => Task.FromResult(GetSize(key));

    public Task<byte[]?> GetAsync(string key, CancellationToken ct = default)
        => Task.FromResult(Get(key));

    public Task<byte[]?> GetSliceAsync(string key, long offset, int length, CancellationToken ct = default)
        => Task.FromResult(GetSlice(key, offset, length));

    public Task<Stream?> GetStreamAsync(string key, long offset = 0, CancellationToken ct = default)
        => Task.FromResult(GetStream(key, offset));

    public async IAsyncEnumerable<(string key, long size)> ListAsync([EnumeratorCancellation]CancellationToken ct = default)
    {
        CheckDisposed();
        var skip = Folder.Length + 1;
        foreach (var x in Directory.EnumerateFiles(Folder, "*.*", SearchOption.AllDirectories))
        {
            await Task.Yield();
            ct.ThrowIfCancellationRequested();
            yield return (key: x.Substring(skip), size: new FileInfo(x).Length);
        }
    }

    public Task RemoveAsync(string key, CancellationToken ct = default)
    {
        Remove(key);
        return Task.CompletedTask;
    }

    public Task FlushAsync(CancellationToken ct = default)
    {
        Flush();
        return Task.CompletedTask;
    }

    public Task<long> GetUsedBytesAsync(Action<long>? onProgress = null, CancellationToken ct = default)
        => Task.FromResult(GetUsedBytes());

    public Task<long> GetReservedBytesAsync(Action<long>? onProgress = null, CancellationToken ct = default)
        => Task.FromResult(GetReservedBytes());

    public Task<Stream> GetWriteStreamAsync(string key, bool overwrite = true, Action<long>? onProgress = null, CancellationToken ct = default)
    {
        throw new NotImplementedException();
    }

    #endregion
}

