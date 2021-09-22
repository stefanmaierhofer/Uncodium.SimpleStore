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

using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Uncodium.SimpleStore
{
    /// <summary>
    /// Uncodium.SimpleStore bindings for Azure blob storage.
    /// </summary>
    public class SimpleAzureBlobStore : ISimpleStore
    {
        private string _sas;
        private BlobContainerClient _client;

        private Stats m_stats;

        private bool m_isDisposed = false;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckDisposed() { if (m_isDisposed) throw new ObjectDisposedException(nameof(SimpleAzureBlobStore)); }

        /// <summary>
        /// Creates a store in the given folder.
        /// </summary>
        public SimpleAzureBlobStore(string sas)
        {
            _sas = sas;

            var uri = new Uri(sas);
            var sasCred = new AzureSasCredential(sas);
            var client = new BlobServiceClient(new Uri($"{uri.Scheme}://{uri.Host}"), sasCred);
            var localPath = uri.LocalPath[0] == '/' ? uri.LocalPath.Substring(1) : uri.LocalPath;
            _client = client.GetBlobContainerClient(localPath);
        }

        public void Dispose()
        {
            CheckDisposed();
            m_isDisposed = true;
        }

        #region ISimpleStore

        /// <summary>
        /// Add data from buffer.
        /// </summary>
        public void Add(string key, byte[] value)
        {
            CheckDisposed();

            _client.GetBlobClient(key).Upload(BinaryData.FromBytes(value));

            Interlocked.Increment(ref m_stats.CountAdd);
            m_stats.LatestKeyAdded = m_stats.LatestKeyFlushed = key;
        }

        /// <summary>
        /// Add data from stream.
        /// </summary>
        public void AddStream(string key, Stream stream, Action<long>? onProgress = default, CancellationToken ct = default)
        {
            var options = new BlobUploadOptions();
            if (onProgress != null) options.ProgressHandler = new Progress<long>(onProgress);

            _client.GetBlobClient(key).Upload(stream, options, ct);

            Interlocked.Increment(ref m_stats.CountAdd);
            m_stats.LatestKeyAdded = m_stats.LatestKeyFlushed = key;
        }

        /// <summary>
        /// True if key exists in store.
        /// </summary>
        public bool Contains(string key)
        {
            CheckDisposed();
            Interlocked.Increment(ref m_stats.CountContains);
            return _client.GetBlobClient(key).Exists().Value;
        }

        /// <summary>
        /// Get size of value in bytes,
        /// or null if key does not exist.
        /// </summary>
        public long? GetSize(string key)
        {
            CheckDisposed();
            return _client.GetBlobClient(key).GetProperties().Value.ContentLength;
        }

        /// <summary>
        /// Get value as buffer,
        /// or null if key does not exist.
        /// </summary>
        public byte[]? Get(string key)
        {
            CheckDisposed();

            try
            {
                var res = _client.GetBlobClient(key).DownloadContent();
                if (res.Value != null)
                {
                    Interlocked.Increment(ref m_stats.CountGet);
                    return res.Value.Content.ToArray();
                }
                else
                {
                    Interlocked.Increment(ref m_stats.CountGetInvalidKey);
                    return null;
                }
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
                var range = new HttpRange(offset, size);
                var res = _client.GetBlobClient(key).DownloadStreaming(range);
                var br = new BinaryReader(res.Value.Content);
                var buffer = br.ReadBytes(size);

                Interlocked.Increment(ref m_stats.CountGetSlice);
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
                var range = new HttpRange(offset);
                var res = _client.GetBlobClient(key).DownloadStreaming(range);
                var stream = res.Value.Content;
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
            foreach (var page in _client.GetBlobs())
            {
                yield return (page.Name, page.Properties.ContentLength ?? 0);
            }
        }

        /// <summary>
        /// Remove entry.
        /// </summary>
        public void Remove(string key)
        {
            CheckDisposed();

            try
            {
                _client.DeleteBlob(key);
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
        public long GetUsedBytes() => List().Sum(x => x.size);

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
    }
}
