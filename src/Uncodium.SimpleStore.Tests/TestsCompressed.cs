using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Uncodium.SimpleStore.Tests
{
    [TestFixture]
    public class TestsCompressed
    {
        private const string TestStoreSmallPath = @"teststore_compressed";
        private const string TestStoreLargePath = @"teststore_large_compressed";

        [OneTimeSetUp]
        public void Init()
        {
            if (Directory.Exists(TestStoreSmallPath))
            {
                Directory.Delete(TestStoreSmallPath, true);
            }
            if (Directory.Exists(TestStoreLargePath))
            {
                Directory.Delete(TestStoreLargePath, true);
            }
        }

        #region Construction

        [Test]
        public void CanCreateMemStore()
        {
            using var store = new SimpleMemoryStore().Compress(CompressionLevel.Fastest);
        }

        [Test]
        public void CanCreateDiskStore()
        {
            using var store = new SimpleDiskStore(TestStoreSmallPath).Compress(CompressionLevel.Fastest);
        }
        
        [Test]
        public void CanCreateDiskStore2()
        {
            using var store = new SimpleDiskStore(TestStoreSmallPath).Compress(CompressionLevel.Fastest);
        }

        #endregion

        #region Add

        [Test]
        public void CanAddMemStore()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleMemoryStore().Compress(CompressionLevel.Fastest);
            store.Add(key, "b", null);
        }

        [Test]
        public void CanAddDiskStore()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleDiskStore(TestStoreSmallPath).Compress(CompressionLevel.Fastest);
            store.Add(key, "b", () => Encoding.UTF8.GetBytes("b"));
        }

        [Test]
        public void CanAddDiskStore2()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleDiskStore(TestStoreSmallPath).Compress(CompressionLevel.Fastest);
            store.Add(key, "b");
        }

        #endregion

        #region Get

        [Test]
        public void CanGetMemStore()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleMemoryStore().Compress(CompressionLevel.Fastest);
            store.Add(key, "b", null);
            var x = store.Get(key);
            Assert.IsTrue(x == null);
        }

        [Test]
        public void CanGetMemStore2()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleMemoryStore().Compress(CompressionLevel.Fastest);
            store.Add(key, "b", () => Encoding.UTF8.GetBytes("b"));
            var x = store.Get(key);
            Assert.IsTrue(Encoding.UTF8.GetString(x) == "b");
        }

        [Test]
        public void CanGetDiskStore()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleDiskStore(TestStoreSmallPath).Compress(CompressionLevel.Fastest);
            store.Add(key, "b", null);
            var x = store.Get(key);
            Assert.IsTrue(x == null);
        }

        [Test]
        public void CanGetDiskStore2()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleDiskStore(TestStoreSmallPath).Compress(CompressionLevel.Fastest);
            store.Add(key, "b", () => Encoding.UTF8.GetBytes("b"));
            var x = store.Get(key);
            Assert.IsTrue(Encoding.UTF8.GetString(x) == "b");
        }

        #endregion

        #region Remove

        [Test]
        public void CanRemoveMemStore()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleMemoryStore().Compress(CompressionLevel.Fastest);
            store.Add(key, "b", null);
            var x = store.Get(key);
            Assert.IsTrue(x == null);

            store.Remove(key);
            var y = store.Get(key);
            Assert.IsTrue(y == null);
        }

        [Test]
        public void CanRemoveMemStore2()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleMemoryStore().Compress(CompressionLevel.Fastest);
            store.Add(key, "b", () => Encoding.UTF8.GetBytes("b"));
            var x = store.Get(key);
            Assert.IsTrue(Encoding.UTF8.GetString(x) == "b");

            store.Remove(key);
            var y = store.Get(key);
            Assert.IsTrue(y == null);
        }

        [Test]
        public void CanRemoveDiskStore()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleDiskStore(TestStoreSmallPath).Compress(CompressionLevel.Fastest);
            store.Add(key, "b", null);
            var x = store.Get(key);
            Assert.IsTrue(x == null);

            store.Remove(key);
            var y = store.Get(key);
            Assert.IsTrue(y == null);
        }

        [Test]
        public void CanRemoveDiskStore2()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleDiskStore(TestStoreSmallPath).Compress(CompressionLevel.Fastest);
            store.Add(key, "b", () => Encoding.UTF8.GetBytes("b"));
            var x = store.Get(key);
            Assert.IsTrue(Encoding.UTF8.GetString(x) == "b");

            store.Remove(key);
            var y = store.Get(key);
            Assert.IsTrue(y == null);
        }

        #endregion

        #region TryGetFromCache

        [Test]
        public void CanTryGetFromCacheMemStore()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleMemoryStore().Compress(CompressionLevel.Fastest);
            store.Add(key, "b", null);
            var x = (string)store.TryGetFromCache(key);
            Assert.IsTrue(x == "b");
        }

        [Test]
        public void CanTryGetFromCacheDiskStore()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleDiskStore(TestStoreSmallPath).Compress(CompressionLevel.Fastest);
            store.Add(key, "b", null);
            var x = (string)store.TryGetFromCache(key);
            Assert.IsTrue(x == "b");
        }

        #endregion

        #region Add parallel

        [Test]
        public void CanAddParallelMemStore()
        {
            using var store = new SimpleMemoryStore().Compress(CompressionLevel.Fastest);
            var stats0 = store.Stats;
            Assert.IsTrue(stats0.CountAdd == 0);

            var ts = new List<Task>();
            for (var t = 0; t < 4; t++)
            {
                ts.Add(Task.Run(() =>
                {
                    for (var i = 0; i < 250000; i++)
                    {
                        var key = Guid.NewGuid().ToString();
                        store.Add(key, "value");
                    }
                }));
            }

            Task.WhenAll(ts).Wait();

            Assert.IsTrue(store.Stats.CountAdd == 1000000);
        }

        [Test]
        public void CanAddParallelDiskStore()
        {
            using var store = new SimpleDiskStore(TestStoreLargePath).Compress(CompressionLevel.Fastest);
            var stats0 = store.Stats;
            Assert.IsTrue(stats0.CountAdd == 0);

            var ts = new List<Task>();
            for (var t = 0; t < 4; t++)
            {
                ts.Add(Task.Run(() =>
                {
                    for (var i = 0; i < 50000; i++)
                    {
                        var key = Guid.NewGuid().ToString();
                        var data = new byte[1024];
                        store.Add(key, data);
                    }
                }));
            }

            Task.WhenAll(ts).Wait();

            Assert.IsTrue(store.Stats.CountAdd == 200000);
        }

        #endregion
    }
}
