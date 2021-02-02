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
            using var store = new SimpleDiskStore(TestStoreSmallPath);
        }
        
        //[Test]
        //public void DiskStoreCreatesIndexFileImmediatelyOnStoreCreation()
        //{
        //    var folder = Path.GetFullPath($"store_{Guid.NewGuid()}");

        //    try
        //    {
        //        var store = new SimpleDiskStore(folder);
        //        Assert.True(File.Exists(Path.Combine(folder, "index.bin")));
        //        store.Dispose();

        //        var store2 = new SimpleDiskStore(folder);
        //        Assert.True(store2.SnapshotKeys().Length == 0);
        //        store2.Dispose();

        //    }
        //    finally
        //    {
        //        Directory.Delete(folder, true);
        //    }
        //}

        [Test]
        public void CanOpenDiskStoreTwiceReadonly()
        {
            using var store = new SimpleDiskStore(TestStoreSmallPath);
            using var storeReadOnly1 = SimpleDiskStore.OpenReadOnlySnapshot(TestStoreSmallPath);
            using var storeReadOnly2 = SimpleDiskStore.OpenReadOnlySnapshot(TestStoreSmallPath);
        }

        //[Test]
        //public void CreateDiskStoreCreatesFolderWithBinFile()
        //{
        //    var path = Path.GetFullPath(Guid.NewGuid().ToString());

        //    Assert.False(Directory.Exists(path));

        //    var store = new SimpleDiskStore(path);

        //    Assert.True(Directory.Exists(path));

        //    store.Dispose();
        //    Directory.Delete(path, true);
        //    Thread.Sleep(250);
        //    Assert.False(Directory.Exists(path));
        //}

        [Test]
        public void CreateDiskStoreCreatesDataFile()
        {
            var path = Path.GetFullPath(Guid.NewGuid().ToString());
            var dataFileName = path + SimpleDiskStore.DefaultFileExtension;

            Assert.False(File.Exists(dataFileName));
            Assert.False(Directory.Exists(dataFileName));

            var store = new SimpleDiskStore(path);

            Assert.True(File.Exists(dataFileName));
            Assert.False(Directory.Exists(dataFileName));

            store.Dispose();
            File.Delete(dataFileName);
            Thread.Sleep(250);

            Assert.False(File.Exists(dataFileName));
            Assert.False(Directory.Exists(dataFileName));
        }

        [Test]
        public void CreateDiskStoreCreatesDataFile_WithExtension()
        {
            var path = Path.GetFullPath(Guid.NewGuid().ToString()) + SimpleDiskStore.DefaultFileExtension;
            var dataFileName = path;

            Assert.False(File.Exists(dataFileName));
            Assert.False(Directory.Exists(dataFileName));

            var store = new SimpleDiskStore(path);

            Assert.True(File.Exists(dataFileName));
            Assert.False(Directory.Exists(dataFileName));

            store.Dispose();
            File.Delete(dataFileName);
            Thread.Sleep(250);

            Assert.False(File.Exists(dataFileName));
            Assert.False(Directory.Exists(dataFileName));
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
            using var store = new SimpleDiskStore(TestStoreSmallPath);
            store.Add(key, "b", () => Encoding.UTF8.GetBytes("b"));
        }

        [Test]
        public void CanAddDiskStore2()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleDiskStore(TestStoreSmallPath);
            store.Add(key, "b");
        }

        [Test]
        public void CanAddDiskStoreMany()
        {
            if (Directory.Exists(TestStoreSmallPath)) Directory.Delete(TestStoreSmallPath, true);
            using var store = new SimpleDiskStore(TestStoreSmallPath);

            var data = new Dictionary<string, string>();

            var imax = 256 * 1024 / (2 + 6 + 8 + 4);
            for (var i = 0; i < imax; i++)
            {
                var key = Guid.NewGuid().ToString();
                var value = $"many {i}";
                store.Add(key, value);
                data[key] = value;
            }
            var keys = store.SnapshotKeys();
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
            var v = Encoding.UTF8.GetString(x);
            Assert.IsTrue(v == "b");
        }

        [Test]
        public void CanGetDiskStoreMany()
        {
            if (Directory.Exists(TestStoreSmallPath)) Directory.Delete(TestStoreSmallPath, true);
            using var store = new SimpleDiskStore(TestStoreSmallPath);

            var data = new Dictionary<string, string>();

            var imax = 256 * 1024 / (2 + 6 + 8 + 4);
            for (var i = 0; i < imax; i++)
            {
                var key = Guid.NewGuid().ToString();
                var value = $"many {i}";
                store.Add(key, value);
                data[key] = value;
            }

            foreach (var kv in data)
            {
                var v = Encoding.UTF8.GetString(store.Get(kv.Key));
                var x = data[kv.Key];
                Assert.True(v == x);
            }
        }

        [Test]
        public void CanGetDiskStore()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleDiskStore(TestStoreSmallPath);
            store.Add(key, "b", null);
            var x = store.Get(key);
            Assert.IsTrue(x == null); 
        }

        [Test]
        public void CanGetDiskStore2()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleDiskStore(TestStoreSmallPath);
            store.Add(key, "b", () => Encoding.UTF8.GetBytes("b"));
            var x = store.Get(key);
            var v = Encoding.UTF8.GetString(x);
            Assert.IsTrue(v == "b");
        }

        #endregion

        #region GetSlice

        private bool ElementsEqual(byte[] xs, byte[] ys)
        {
            if (xs == null || ys == null || xs.Length != ys.Length) return false;
            for (var i = 0; i < xs.Length; i++) if (xs[i] != ys[i]) return false;
            return true;
        }
        private void CheckGetSlice(ISimpleStore store)
        {
            var key = Guid.NewGuid().ToString();
            store.Add(key, new byte[] { 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 });
            Assert.IsTrue(ElementsEqual(store.GetSlice(key, 0, 10), new byte[] { 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 }));
            Assert.IsTrue(ElementsEqual(store.GetSlice(key, 0, 1), new byte[] { 10 }));
            Assert.IsTrue(ElementsEqual(store.GetSlice(key, 9, 1), new byte[] { 19 }));
            Assert.IsTrue(ElementsEqual(store.GetSlice(key, 4, 4), new byte[] { 14, 15, 16, 17 }));
        }
        [Test]
        public void CanGetSliceMemStore()
        {
            using var store = new SimpleMemoryStore();
            CheckGetSlice(store);
        }

        [Test]
        public void CanGetSliceDiskStore()
        {
            using var store = new SimpleDiskStore(TestStoreSmallPath);
            CheckGetSlice(store);
        }

        #endregion

        #region OpenReadStream

        //private void CheckOpenReadStream(ISimpleStore store)
        //{
        //    var key = Guid.NewGuid().ToString();
        //    store.Add(key, new byte[] { 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 });
        //    using var stream = store.OpenReadStream(key);
        //    using var br = new BinaryReader(stream);
        //    Assert.IsTrue(ElementsEqual(br.ReadBytes(10), new byte[] { 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 }));

        //    stream.Seek(0, SeekOrigin.Begin);
        //    Assert.IsTrue(ElementsEqual(br.ReadBytes(1), new byte[] { 10 }));

        //    stream.Seek(9, SeekOrigin.Begin);
        //    Assert.IsTrue(ElementsEqual(br.ReadBytes(1), new byte[] { 19 }));

        //    stream.Seek(4, SeekOrigin.Begin);
        //    Assert.IsTrue(ElementsEqual(br.ReadBytes(4), new byte[] { 14, 15, 16, 17 }));
        //}

        [Test]
        public void CanOpenReadStreamMemStore()
        {
            using var store = new SimpleMemoryStore();
            CheckGetSlice(store);
        }

        [Test]
        public void CanOpenReadStreamDiskStore()
        {
            using var store = new SimpleDiskStore(TestStoreSmallPath);
            CheckGetSlice(store);
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
            using var store = new SimpleDiskStore(TestStoreSmallPath);
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
            using var store = new SimpleDiskStore(TestStoreSmallPath);
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
            using var store = new SimpleDiskStore(TestStoreSmallPath);
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
            using var store = new SimpleDiskStore(TestStoreLargePath);
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
            using var store = new SimpleDiskStore(TestStoreLargePath);
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
            if (File.Exists(storename + SimpleDiskStore.DefaultFileExtension)) File.Delete(storename + SimpleDiskStore.DefaultFileExtension);

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
        }

        #endregion
    }
}
