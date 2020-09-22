using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Uncodium.SimpleStore.Tests
{
    [TestFixture]
    public class Tests
    {
        private const string TestStoreSmallPath = @"teststore";
        private const string TestStoreLargePath = @"teststore_large";

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
            using var store = new SimpleMemoryStore();
        }

        [Test]
        public void CanCreateDiskStore()
        {
            using var store = new SimpleDiskStore(TestStoreSmallPath, null);
        }
        
        [Test]
        public void CanCreateDiskStore2()
        {
            using var store = new SimpleDiskStore(TestStoreSmallPath);
        }

        [Test]
        public void CanOpenDiskStoreTwiceReadonly()
        {
            using var store = new SimpleDiskStore(TestStoreSmallPath);
            using var storeReadOnly = SimpleDiskStore.OpenReadOnlySnapshot(TestStoreSmallPath);
        }

        [Test]
        public void CreateDiskStoreCreatesFolderWithBinFile()
        {
            var path = Path.GetFullPath(Guid.NewGuid().ToString());

            Assert.False(Directory.Exists(path));

            var store = new SimpleDiskStore(path);

            Assert.True(Directory.Exists(path));

            store.Dispose();
            Directory.Delete(path, true);
            Thread.Sleep(250);
            Assert.False(Directory.Exists(path));
        }

        #endregion

        #region Add

        [Test]
        public void CanAddMemStore()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleMemoryStore();
            store.Add(key, "b", null);
        }

        [Test]
        public void CanAddDiskStore()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleDiskStore(TestStoreSmallPath, null);
            store.Add(key, "b", () => Encoding.UTF8.GetBytes("b"));
        }

        [Test]
        public void CanAddDiskStore2()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleDiskStore(TestStoreSmallPath, null);
            store.Add(key, "b");
        }

        #endregion

        #region Get

        [Test]
        public void CanGetMemStore()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleMemoryStore();
            store.Add(key, "b", null);
            var x = store.Get(key);
            Assert.IsTrue(x == null);
        }

        [Test]
        public void CanGetMemStore2()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleMemoryStore();
            store.Add(key, "b", () => Encoding.UTF8.GetBytes("b"));
            var x = store.Get(key);
            Assert.IsTrue(Encoding.UTF8.GetString(x) == "b");
        }

        [Test]
        public void CanGetDiskStore()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleDiskStore(TestStoreSmallPath, null);
            store.Add(key, "b", null);
            var x = store.Get(key);
            Assert.IsTrue(x == null);
        }

        [Test]
        public void CanGetDiskStore2()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleDiskStore(TestStoreSmallPath, null);
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
            using var store = new SimpleMemoryStore();
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
            using var store = new SimpleMemoryStore();
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
            using var store = new SimpleDiskStore(TestStoreSmallPath, null);
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
            using var store = new SimpleDiskStore(TestStoreSmallPath, null);
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
            using var store = new SimpleMemoryStore();
            store.Add(key, "b", null);
            var x = (string)store.TryGetFromCache(key);
            Assert.IsTrue(x == "b");
        }

        [Test]
        public void CanTryGetFromCacheDiskStore()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleDiskStore(TestStoreSmallPath, null);
            store.Add(key, "b", null);
            var x = (string)store.TryGetFromCache(key);
            Assert.IsTrue(x == "b");
        }

        #endregion

        #region Add parallel

        [Test]
        public void CanAddParallelMemStore()
        {
            using var store = new SimpleMemoryStore();
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
            using var store = new SimpleDiskStore(TestStoreLargePath, null);
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

        [Test]
        public void CanAddAndGetMultiThreadedDiskStore()
        {
            using var store = new SimpleDiskStore(TestStoreLargePath, null);
            var stats0 = store.Stats;

            var keys = new List<string>();

            var ts = new List<Task>();
            for (var t = 0; t < 4; t++)
            {
                ts.Add(Task.Run(() =>
                {
                    var r = new Random();
                    for (var i = 0; i < 5000; i++)
                    {
                        if (r.NextDouble() < 0.5)
                        {
                            //Console.WriteLine($"W {Thread.CurrentThread.ManagedThreadId}");
                            var key = Guid.NewGuid().ToString();
                            lock (keys) keys.Add(key);
                            var data = new byte[r.Next(100000)];
                            store.Add(key, data);
                        }
                        else
                        {
                            if (keys.Count == 0) continue;
                            //Console.WriteLine($"R {Thread.CurrentThread.ManagedThreadId}");
                            string key;
                            lock (keys) key = keys[r.Next(keys.Count)];
                            var data = store.Get(key);
                        }

                        if (i % 1000 == 0) Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}] {i}");
                    }
                }));
            }

            Task.WhenAll(ts).Wait();

            Assert.IsTrue(store.Stats.CountAdd == keys.Count);
        }

        #endregion

        #region SnapshotKeys

        [Test]
        public void CanSnapshotKeys_Memory()
        {
            var key1 = Guid.NewGuid().ToString();
            var key2 = Guid.NewGuid().ToString();
            var key3 = Guid.NewGuid().ToString();
            using var store = new SimpleMemoryStore();
            store.Add(key1, "key1", null);
            Assert.IsTrue(store.SnapshotKeys().Length == 1);
            store.Add(key2, "key2", null);
            Assert.IsTrue(store.SnapshotKeys().Length == 2);
            store.Add(key3, "key3", null);
            Assert.IsTrue(store.SnapshotKeys().Length == 3);

            var keys = new HashSet<string>(store.SnapshotKeys());
            Assert.IsTrue(keys.Contains(key1));
            Assert.IsTrue(keys.Contains(key2));
            Assert.IsTrue(keys.Contains(key3));
        }

        [Test]
        public void CanSnapshotKeys_Disk()
        {
            var key1 = Guid.NewGuid().ToString();
            var key2 = Guid.NewGuid().ToString();
            var key3 = Guid.NewGuid().ToString();
            var storename = TestStoreSmallPath + ".1";
            if (Directory.Exists(storename)) Directory.Delete(storename, true);

            using var store = new SimpleDiskStore(storename);
            store.Add(key1, "key1", () => Encoding.UTF8.GetBytes("key1"));
            Assert.IsTrue(store.SnapshotKeys().Length == 1);
            store.Add(key2, "key2", () => Encoding.UTF8.GetBytes("key2"));
            Assert.IsTrue(store.SnapshotKeys().Length == 2);
            store.Add(key3, "key3", () => Encoding.UTF8.GetBytes("key3"));
            Assert.IsTrue(store.SnapshotKeys().Length == 3);

            var keys = new HashSet<string>(store.SnapshotKeys());
            Assert.IsTrue(keys.Contains(key1));
            Assert.IsTrue(keys.Contains(key2));
            Assert.IsTrue(keys.Contains(key3));

            store.Flush();
        }

        #endregion
    }
}
