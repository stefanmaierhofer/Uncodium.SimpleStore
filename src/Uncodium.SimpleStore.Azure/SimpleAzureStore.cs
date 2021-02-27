using System;
using System.IO;

namespace Uncodium.SimpleStore.Azure
{
    public class SimpleAzureStore : ISimpleStore
    {
        public Stats Stats => throw new NotImplementedException();

        public string LatestKeyAdded => throw new NotImplementedException();

        public string LatestKeyFlushed => throw new NotImplementedException();

        public string Version => throw new NotImplementedException();

        public void Add(string key, object value, uint flags, Func<byte[]> getEncodedValue)
        {
            throw new NotImplementedException();
        }

        public bool Contains(string key)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public void Flush()
        {
            throw new NotImplementedException();
        }

        public byte[] Get(string key)
        {
            throw new NotImplementedException();
        }

        public long GetReservedBytes()
        {
            throw new NotImplementedException();
        }

        public byte[] GetSlice(string key, long offset, int length)
        {
            throw new NotImplementedException();
        }

        public long GetUsedBytes()
        {
            throw new NotImplementedException();
        }

        public Stream OpenReadStream(string key)
        {
            throw new NotImplementedException();
        }

        public void Remove(string key)
        {
            throw new NotImplementedException();
        }

        public string[] SnapshotKeys()
        {
            throw new NotImplementedException();
        }

        public object TryGetFromCache(string key)
        {
            throw new NotImplementedException();
        }
    }
}
