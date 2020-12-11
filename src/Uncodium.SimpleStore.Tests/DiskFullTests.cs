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
    }
}
