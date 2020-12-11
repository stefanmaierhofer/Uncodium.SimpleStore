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
    public class DiskFullTests
    {
        private void TestWithFreshStore(Action<string, SimpleDiskStore> test)
        {
            // random store folder
            var folder = Path.GetFullPath($"store_{Guid.NewGuid()}");
            SimpleDiskStore store = null;
            try
            {
                // create fresh store
                store = new SimpleDiskStore(folder);

                // run test
                test(folder, store);
            }
            finally
            {
                try { store.Dispose(); } catch { }
                Directory.Delete(folder, true);
            }
        }
        private string GetString(SimpleDiskStore store, string key)
        {
            var buffer = store.Get(key);
            return buffer != null ? Encoding.UTF8.GetString(buffer) : null;
        }

        [Test]
        public void DiskStoreThrowsExceptionOnFullDisk() => TestWithFreshStore((folder, store) =>
        {
            store.SimulateFullDiskOnNextResize = true;
            Assert.Throws<IOException>(() => store.Add("foo", "bar"));
        });

        [Test]
        public void DiskStoreContainsLastEntriesBeforeDiskFull() => TestWithFreshStore((folder, store) =>
        {
            store.Add("last", "entry");
            store.Flush();

            store.Add("afterLastFlush", "mmmh");

            store.SimulateFullDiskOnNextResize = true;
            Assert.Throws<IOException>(() => store.Add("foo", "bar"));
            store.SimulateFullDiskOnNextResize = false;

            var x = GetString(store, "last");
            Assert.True(x == "entry");

            // can access entry that has not been flushed
            var y = GetString(store, "afterLastFlush");
            Assert.True(y == "mmmh");
        });

        [Test]
        public void DiskStoreContainsLastEntryFlushedBeforeDiskFull_AfterReopen() => TestWithFreshStore((folder, store) =>
        {
            store.Add("last", "entry");
            store.Flush();

            store.Add("afterLastFlush", "mmmh");

            store.SimulateFullDiskOnNextResize = true;
            Assert.Throws<IOException>(() => store.Add("foo", "bar"));
            store.SimulateFullDiskOnNextResize = false;

            using var reopenedStore = SimpleDiskStore.OpenReadOnlySnapshot(folder);

            var x = GetString(reopenedStore, "last");
            Assert.True(x == "entry");

            var y = GetString(reopenedStore, "afterLastFlush");
            Assert.True(y == null);
        });

        [Test]
        public void ContainsFailsWhileDiskFull() => TestWithFreshStore((folder, store) =>
        {
            store.Add("foo", "bar");

            store.SimulateFullDiskOnNextResize = true;
            Assert.Throws<IOException>(() => store.Add("last drop", "to force disk full"));
            Assert.Throws<IOException>(() => store.Contains("foo"));
            Assert.Throws<IOException>(() => store.Contains("nonexisting"));
            store.SimulateFullDiskOnNextResize = false;

            Assert.True(store.Contains("foo"));
            Assert.False(store.Contains("nonexisting"));
        });

        [Test]
        public void GetFailsWhileDiskFull() => TestWithFreshStore((folder, store) =>
        {
            store.Add("foo", "bar");

            store.SimulateFullDiskOnNextResize = true;
            Assert.Throws<IOException>(() => store.Add("last drop", "to force disk full"));
            Assert.Throws<IOException>(() => store.Get("foo"));
            Assert.Throws<IOException>(() => store.Get("nonexisting"));
            store.SimulateFullDiskOnNextResize = false;

            Assert.True(GetString(store, "foo") == "bar");
            Assert.True(GetString(store, "nonexisting") == null);
        });

        [Test]
        public void GetSliceFailsWhileDiskFull() => TestWithFreshStore((folder, store) =>
        {
            store.Add("foo", "bar");

            store.SimulateFullDiskOnNextResize = true;
            Assert.Throws<IOException>(() => store.Add("last drop", "to force disk full"));
            Assert.Throws<IOException>(() => store.GetSlice("foo", 0, 1));
            Assert.Throws<IOException>(() => store.GetSlice("nonexisting", 0, 1));
            store.SimulateFullDiskOnNextResize = false;

            Assert.True(store.GetSlice("foo", 0, 1).Length == 1);
            Assert.True(store.GetSlice("nonexisting", 0, 1) == null);
        });

        [Test]
        public void OpenReadStreamFailsWhileDiskFull() => TestWithFreshStore((folder, store) =>
        {
            store.Add("foo", "bar");

            store.SimulateFullDiskOnNextResize = true;
            Assert.Throws<IOException>(() => store.Add("last drop", "to force disk full"));
            Assert.Throws<IOException>(() => store.OpenReadStream("foo"));
            Assert.Throws<IOException>(() => store.OpenReadStream("nonexisting"));
            store.SimulateFullDiskOnNextResize = false;

            using var s1 = store.OpenReadStream("foo");
            Assert.True(s1 != null);

            using var s2 = store.OpenReadStream("nonexisting");
            Assert.True(s2 == null);
        });

        [Test]
        public void RemoveFailsWhileDiskFull() => TestWithFreshStore((folder, store) =>
        {
            store.Add("foo", "bar");

            store.SimulateFullDiskOnNextResize = true;
            Assert.Throws<IOException>(() => store.Add("last drop", "to force disk full"));
            Assert.Throws<IOException>(() => store.Remove("foo"));
            Assert.Throws<IOException>(() => store.Remove("nonexisting"));
            store.SimulateFullDiskOnNextResize = false;

            store.Remove("foo");
            Assert.True(GetString(store, "foo") == null);

            store.Remove("nonexisting");
            Assert.True(GetString(store, "nonexisting") == null);
        });

        [Test]
        public void TryGetFromCacheStillWorksWhileDiskFull() => TestWithFreshStore((folder, store) =>
        {
            store.Add("foo", "bar");

            store.SimulateFullDiskOnNextResize = true;
            Assert.Throws<IOException>(() => store.Add("last drop", "to force disk full"));
            Assert.True(store.TryGetFromCache("foo") != null);
            Assert.True(store.TryGetFromCache("nonexisting") == null);
            store.SimulateFullDiskOnNextResize = false;

            Assert.True(store.TryGetFromCache("foo") != null);
            Assert.True(store.TryGetFromCache("nonexisting") == null);
        });

        [Test]
        public void SnapshotKeysStillWorksWhileDiskFull() => TestWithFreshStore((folder, store) =>
        {
            store.Add("foo", "bar");

            store.SimulateFullDiskOnNextResize = true;
            Assert.Throws<IOException>(() => store.Add("last drop", "to force disk full"));
            Assert.True(store.SnapshotKeys().Length == 1);
            store.SimulateFullDiskOnNextResize = false;

            Assert.True(store.SnapshotKeys().Length == 1);
        });

        [Test]
        public void FlushFailsWhileDiskFull() => TestWithFreshStore((folder, store) =>
        {
            store.Add("foo", "bar");

            store.SimulateFullDiskOnNextResize = true;
            Assert.Throws<IOException>(() => store.Add("last drop", "to force disk full"));
            Assert.Throws<IOException>(() => store.Flush());
            store.SimulateFullDiskOnNextResize = false;

            store.Flush();
        });
    }
}
