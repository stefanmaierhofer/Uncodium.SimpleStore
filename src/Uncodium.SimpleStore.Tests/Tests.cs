﻿using NUnit.Framework;
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
        private static readonly string TestStoreSmallPath = Path.GetFullPath(@"teststore");
        private static readonly string TestStoreLargePath = Path.GetFullPath(@"teststore_large");
        private static readonly string TestStoreFolder    = Path.GetFullPath(@"teststore_folder");

        //[OneTimeSetUp]
        [SetUp]
        [TearDown]
        public void Init()
        {
            if (Directory.Exists(TestStoreSmallPath))
            {
                Directory.Delete(TestStoreSmallPath, true);
            }
            if (File.Exists(TestStoreSmallPath))
            {
                File.Delete(TestStoreSmallPath);
                File.Delete(TestStoreSmallPath + ".log");
            }
            if (Directory.Exists(TestStoreLargePath))
            {
                Directory.Delete(TestStoreLargePath, true);
            }
            if (File.Exists(TestStoreLargePath))
            {
                File.Delete(TestStoreLargePath);
                File.Delete(TestStoreLargePath + ".log");
            }
            if (Directory.Exists(TestStoreFolder))
            {
                Directory.Delete(TestStoreFolder, true);
            }
            if (File.Exists(TestStoreFolder))
            {
                File.Delete(TestStoreFolder);
                File.Delete(TestStoreFolder + ".log");
            }
        }

        #region Utils

        #endregion

        #region Construction

        [Test]
        public void CanCreateMemStore()
        {
            using var store = new SimpleMemoryStore();
        }

        [Test]
        public void CanDisposeMemStore()
        {
            var store = new SimpleMemoryStore();
            Assert.True(store.IsDisposed == false);
            store.Dispose();
            Assert.True(store.IsDisposed == true);
        }

        [Test]
        public void CanCreateDiskStore()
        {
            try
            {
                using var store = new SimpleDiskStore(TestStoreSmallPath);
            }
            finally
            {
                File.Delete(TestStoreSmallPath);
                File.Delete(TestStoreSmallPath + ".log");
            }
        }

        [Test]
        public void CanDisposeDiskStore()
        {
            try
            {
                var store = new SimpleDiskStore(TestStoreSmallPath); 
                Assert.True(store.IsDisposed == false);
                store.Dispose();
                Assert.True(store.IsDisposed == true);
            }
            finally
            {
                File.Delete(TestStoreSmallPath);
                File.Delete(TestStoreSmallPath + ".log");
            }
        }

        [Test]
        public void CanCreateFolderStore()
        {
            try
            {
                using var store = new SimpleFolderStore(TestStoreFolder);
            }
            finally
            {
                Directory.Delete(TestStoreFolder, true);
            }
        }

        [Test]
        public void CanDisposeFolderStore()
        {
            try
            {
                var store = new SimpleFolderStore(TestStoreFolder);
                Assert.True(store.IsDisposed == false);
                store.Dispose();
                Assert.True(store.IsDisposed == true);
            }
            finally
            {
                Directory.Delete(TestStoreFolder, true);
            }
        }

        [Test]
        public void CanOpenDiskStoreTwiceReadonly()
        {
            try
            {
                using var store = new SimpleDiskStore(TestStoreSmallPath);
                using var storeReadOnly1 = SimpleDiskStore.OpenReadOnlySnapshot(TestStoreSmallPath);
                using var storeReadOnly2 = SimpleDiskStore.OpenReadOnlySnapshot(TestStoreSmallPath);
            }
            finally
            {
                //Directory.Delete(TestStoreSmallPath, true);
                File.Delete(TestStoreSmallPath);
                File.Delete(TestStoreSmallPath + ".log");
            }
        }

        [Test]
        public void CreateDiskStoreCreatesDataFile()
        {
            var path = Path.GetFullPath(Guid.NewGuid().ToString());
            var dataFileName = path + ".uds";

            try
            {
                Assert.False(File.Exists(dataFileName));
                Assert.False(Directory.Exists(dataFileName));

                var store = new SimpleDiskStore(dataFileName);

                Assert.True(File.Exists(dataFileName));
                Assert.False(Directory.Exists(dataFileName));

                store.Dispose();
            }
            finally
            {
                File.Delete(dataFileName);
                File.Delete(dataFileName + ".log");
                Thread.Sleep(250);

                Assert.False(File.Exists(dataFileName));
                Assert.False(Directory.Exists(dataFileName));
            }
        }

        [Test]
        public void CreateDiskStoreCreatesDataFile_WithExtension()
        {
            var dataFileName = Path.GetFullPath(Guid.NewGuid().ToString()) + ".uds";

            try
            {
                Assert.False(File.Exists(dataFileName));
                Assert.False(Directory.Exists(dataFileName));

                var store = new SimpleDiskStore(dataFileName);

                Assert.True(File.Exists(dataFileName));
                Assert.False(Directory.Exists(dataFileName));

                store.Dispose();
            }
            finally
            {
                File.Delete(dataFileName);
                File.Delete(dataFileName + ".log");
                Thread.Sleep(250);

                Assert.False(File.Exists(dataFileName));
                Assert.False(Directory.Exists(dataFileName));
            }
        }

        #endregion

        #region Logging

        [Test]
        public void CustomLog()
        {
            try
            {
                var hasLogged = false;
                using var store = new SimpleDiskStore(
                    path: TestStoreSmallPath,
                    readOnlySnapshot: false,
                    logLines: lines => hasLogged = true,
                    initialSizeInBytes: 0L
                    );
                Assert.True(hasLogged);
            }
            finally
            {
                File.Delete(TestStoreSmallPath);
                File.Delete(TestStoreSmallPath + ".log");
            }
        }

        [Test]
        public void CustomLogReadOnlyStore()
        {
            try
            {
                // init empty store
                var initStore = new SimpleDiskStore(path: TestStoreSmallPath);
                initStore.Dispose();

                var hasLogged = false;
                using var store = new SimpleDiskStore(
                    path: TestStoreSmallPath,
                    readOnlySnapshot: true,
                    logLines: lines => hasLogged = true,
                    initialSizeInBytes: 0L
                    );
                Assert.True(hasLogged);
            }
            finally
            {
                File.Delete(TestStoreSmallPath);
                File.Delete(TestStoreSmallPath + ".log");
            }
        }

        [Test]
        public void CustomLogReadOnlyLogFileCollision()
        {
            try
            {
                bool hasLoggedReadWrite = false;
                using var readWriteStore = new SimpleDiskStore(
                    path: TestStoreSmallPath,
                    readOnlySnapshot: false,
                    logLines: lines => hasLoggedReadWrite = true,
                    initialSizeInBytes: 0L
                    );

                bool hasLoggedReadOnly = false;
                using var store = new SimpleDiskStore(
                    path: TestStoreSmallPath,
                    readOnlySnapshot: true,
                    logLines: lines => hasLoggedReadOnly = true,
                    initialSizeInBytes: 0L
                    );

                Assert.True(hasLoggedReadWrite);
                Assert.True(hasLoggedReadOnly);
            }
            finally
            {
                File.Delete(TestStoreSmallPath);
                File.Delete(TestStoreSmallPath + ".log");
            }
        }

        #endregion

        #region Add

        [Test]
        public void CanAddMemStore()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleMemoryStore();
            store.Add(key, "b");
        }

        [Test]
        public void CanAddMemStoreStream()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleMemoryStore();

            var value = "This is a test for adding via stream. This is a test for adding via stream. This is a test for adding via stream.";
            var stream = new MemoryStream(Encoding.UTF8.GetBytes(value));
            store.AddStream(key, stream);

            var value2 = Encoding.UTF8.GetString(store.Get(key));
            Assert.True(value == value2);
        }

        [Test]
        public void CanAddDiskStore()
        {
            var key = Guid.NewGuid().ToString();
            try
            {
                using var store = new SimpleDiskStore(TestStoreSmallPath);
                store.Add(key, "b");
            }
            finally
            {
                File.Delete(TestStoreSmallPath);
                File.Delete(TestStoreSmallPath + ".log");
            }
        }

        [Test]
        public void CanAddDiskStoreStream()
        {
            var key = Guid.NewGuid().ToString();

            try
            {
                using var store = new SimpleDiskStore(TestStoreSmallPath);
                var value = "This is a test for adding via stream. This is a test for adding via stream. This is a test for adding via stream.";
                var stream = new MemoryStream(Encoding.UTF8.GetBytes(value));
                store.AddStream(key, stream);

                var value2 = Encoding.UTF8.GetString(store.Get(key));
                Assert.True(value == value2);
            }
            finally
            {
                File.Delete(TestStoreSmallPath);
                File.Delete(TestStoreSmallPath + ".log");
            }
        }

        [Test]
        public void CanAddFolderStore()
        {
            var key = Guid.NewGuid().ToString();
            try
            {
                using var store = new SimpleFolderStore(TestStoreFolder);
                store.Add(key, "b");
            }
            finally
            {
                Directory.Delete(TestStoreFolder, true);
            }
        }

        [Test]
        public void CanAddFolderStoreStream()
        {
            try
            {
                var key = Guid.NewGuid().ToString();
                using var store = new SimpleFolderStore(TestStoreFolder);
                var value = "This is a test for adding via stream. This is a test for adding via stream. This is a test for adding via stream.";
                var stream = new MemoryStream(Encoding.UTF8.GetBytes(value));
                store.AddStream(key, stream);

                var value2 = Encoding.UTF8.GetString(store.Get(key));
                Assert.True(value == value2);
            }
            finally
            {
                Directory.Delete(TestStoreFolder, true);
            }
        }

        [Test]
        public void CanAddDiskStoreMany()
        {
            if (Directory.Exists(TestStoreSmallPath)) Directory.Delete(TestStoreSmallPath, true);

            try
            {
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
                var keys = store.List().ToArray();
            }
            finally
            {
                File.Delete(TestStoreSmallPath);
                File.Delete(TestStoreSmallPath + ".log");
            }
        }

        [Test]
        public void CanAddMemoryStoreMany()
        {
            using var store = new SimpleMemoryStore();

            var data = new Dictionary<string, string>();

            var imax = 256 * 1024 / (2 + 6 + 8 + 4);
            for (var i = 0; i < imax; i++)
            {
                var key = Guid.NewGuid().ToString();
                var value = $"many {i}";
                store.Add(key, value);
                data[key] = value;
            }
            var keys = store.List().ToArray();
        }

        [Test]
        public void CanAddFolderStoreMany()
        {
            if (Directory.Exists(TestStoreFolder)) Directory.Delete(TestStoreFolder, true);

            try
            {
                using var store = new SimpleFolderStore(TestStoreFolder);
                var data = new Dictionary<string, string>();

                var imax = 256 * 1 / (2 + 6 + 8 + 4);
                for (var i = 0; i < imax; i++)
                {
                    var key = Guid.NewGuid().ToString();
                    var value = $"many {i}";
                    store.Add(key, value);
                    data[key] = value;
                }
                var keys = store.List().ToArray();
            }
            finally
            {
                Directory.Delete(TestStoreFolder, true);
            }
        }

        #endregion

        #region Get

        [Test]
        public void CanGetMemStore()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleMemoryStore();
            store.Add(key, "b");
            var x = store.Get(key);
            var v = Encoding.UTF8.GetString(x);
            Assert.IsTrue(v == "b");
        }

        [Test]
        public void CanGetDiskStoreMany()
        {
            if (Directory.Exists(TestStoreSmallPath)) Directory.Delete(TestStoreSmallPath, true);
            try
            {
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
            finally
            {
                File.Delete(TestStoreSmallPath);
                File.Delete(TestStoreSmallPath + ".log");
            }
        }

        [Test]
        public void CanGetDiskStore()
        {
            var key = Guid.NewGuid().ToString();
            try
            {
                using var store = new SimpleDiskStore(TestStoreSmallPath);
                store.Add(key, Encoding.UTF8.GetBytes("b"));
                var x = store.Get(key);
                var v = Encoding.UTF8.GetString(x);
                Assert.IsTrue(v == "b");
            }
            finally
            {
                File.Delete(TestStoreSmallPath);
                File.Delete(TestStoreSmallPath + ".log");
            }
        }

        #endregion

        #region GetSlice

        private static bool ElementsEqual(byte[] xs, byte[] ys)
        {
            if (xs == null || ys == null || xs.Length != ys.Length) return false;
            for (var i = 0; i < xs.Length; i++) if (xs[i] != ys[i]) return false;
            return true;
        }
        private static void CheckGetSlice(ISimpleStore store)
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
            try
            {
                using var store = new SimpleDiskStore(TestStoreSmallPath);
                CheckGetSlice(store);
            }
            finally
            {
                File.Delete(TestStoreSmallPath);
                File.Delete(TestStoreSmallPath + ".log");
            }
        }

        #endregion

        #region OpenReadStream

        private static void CheckOpenReadStream(ISimpleStore store)
        {
            var key = Guid.NewGuid().ToString();
            store.Add(key, new byte[] { 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 });
            using var stream = store.GetStream(key);
            using var br = new BinaryReader(stream);

            var buffer_ = store.Get(key);
            var buffer = br.ReadBytes(10);
            Assert.IsTrue(ElementsEqual(buffer, new byte[] { 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 }));

            stream.Seek(0, SeekOrigin.Begin);
            Assert.IsTrue(ElementsEqual(br.ReadBytes(1), new byte[] { 10 }));

            stream.Seek(9, SeekOrigin.Begin);
            Assert.IsTrue(ElementsEqual(br.ReadBytes(1), new byte[] { 19 }));

            stream.Seek(4, SeekOrigin.Begin);
            Assert.IsTrue(ElementsEqual(br.ReadBytes(4), new byte[] { 14, 15, 16, 17 }));
        }

        [Test]
        public void CanOpenReadStreamMemStore()
        {
            using var store = new SimpleMemoryStore();
            CheckOpenReadStream(store);
        }

        [Test]
        public void CanOpenReadStreamDiskStore()
        {
            try
            {
                using var store = new SimpleDiskStore(TestStoreSmallPath);
                CheckOpenReadStream(store);
            }
            finally
            {
                File.Delete(TestStoreSmallPath);
                File.Delete(TestStoreSmallPath + ".log");
            }
        }


        [Test]
        public void CanOpenReadStreamFolderStore()
        {
            try
            {
                using var store = new SimpleFolderStore(TestStoreFolder);
                CheckOpenReadStream(store);
            }
            finally
            {
                Directory.Delete(TestStoreFolder, true);
            }
        }

        #endregion

        #region Remove

        [Test]
        public void CanRemoveMemStore()
        {
            var key = Guid.NewGuid().ToString();
            using var store = new SimpleMemoryStore();
            store.Add(key, "b");
            var x = store.Get(key);
            Assert.IsTrue(Encoding.UTF8.GetString(x) == "b");

            store.Remove(key);
            var y = store.Get(key);
            Assert.IsTrue(y == null);
        }

        [Test]
        public void CanRemoveDiskStore()
        {
            try
            {
                var key = Guid.NewGuid().ToString();
                using var store = new SimpleDiskStore(TestStoreSmallPath);
                store.Add(key, "b");
                var x = store.Get(key);
                Assert.IsTrue(Encoding.UTF8.GetString(x) == "b");

                store.Remove(key);
                var y = store.Get(key);
                Assert.IsTrue(y == null);
            }
            finally
            {
                File.Delete(TestStoreSmallPath);
                File.Delete(TestStoreSmallPath + ".log");
            }
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
            try
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
            finally
            {
                File.Delete(TestStoreLargePath);
                File.Delete(TestStoreLargePath + ".log");
            }
        }

        [Test]
        public void CanAddAndGetMultiThreadedDiskStore()
        {
            try
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
                            //Console.WriteLine($"W {Environment.CurrentManagedThreadId}");
                            var key = Guid.NewGuid().ToString();
                                lock (keys) keys.Add(key);
                                var data = new byte[r.Next(100000)];
                                store.Add(key, data);
                            }
                            else
                            {
                                if (keys.Count == 0) continue;
                            //Console.WriteLine($"R {Environment.CurrentManagedThreadId}");
                            string key;
                                lock (keys) key = keys[r.Next(keys.Count)];
                                var data = store.Get(key);
                            }

                        //if (i % 1000 == 0) Console.WriteLine($"[{Environment.CurrentManagedThreadId}] {i}");
                    }
                    }));
                }

                Task.WhenAll(ts).Wait();

                Assert.IsTrue(store.Stats.CountAdd == keys.Count);
            }
            finally
            {
                File.Delete(TestStoreLargePath);
                File.Delete(TestStoreLargePath + ".log");
            }
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
            store.Add(key1, "key1");
            Assert.IsTrue(store.List().Count() == 1);
            store.Add(key2, "key2");
            Assert.IsTrue(store.List().Count() == 2);
            store.Add(key3, "key3");
            Assert.IsTrue(store.List().Count() == 3);

            var keys = new HashSet<string>(store.List().Select(x => x.key));
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
            try
            {
                //Directory.Delete(storename, true);
                File.Delete(storename);
                File.Delete(storename + ".log");

                using var store = new SimpleDiskStore(storename);
                store.Add(key1, "key1");
                Assert.IsTrue(store.List().Count() == 1);
                store.Add(key2, "key2");
                Assert.IsTrue(store.List().Count() == 2);
                store.Add(key3, "key3");
                Assert.IsTrue(store.List().Count() == 3);

                var keys = new HashSet<string>(store.List().Select(x => x.key));
                Assert.IsTrue(keys.Contains(key1));
                Assert.IsTrue(keys.Contains(key2));
                Assert.IsTrue(keys.Contains(key3));
            }
            finally
            {
                //Directory.Delete(storename, true);
                File.Delete(storename);
                File.Delete(storename + ".log");
            }
        }

        #endregion
    }
}
