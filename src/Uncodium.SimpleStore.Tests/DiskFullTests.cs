using NUnit.Framework;
using System;
using System.IO;
using System.Linq;
using System.Text;

namespace Uncodium.SimpleStore.Tests
{
    [TestFixture]
    public class DiskFullTests
    {
        private static void TestWithFreshStore(Action<string, SimpleDiskStore> test)
        {
            var path = Path.Combine(Path.GetTempPath(), $"simple-store-disk-full-{Guid.NewGuid():N}");
            SimpleDiskStore store = null;
            try
            {
                // create fresh store
                store = new SimpleDiskStore(path);

                // run test
                test(path, store);
            }
            finally
            {
                store?.Dispose();
                File.Delete(path);
                File.Delete(path + ".log");
            }
        }
        private static string GetString(SimpleDiskStore store, string key)
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
            Assert.That(x, Is.EqualTo("entry"));

            // can access entry that has not been flushed
            var y = GetString(store, "afterLastFlush");
            Assert.That(y, Is.EqualTo("mmmh"));
        });

        [Test]
        [Platform("Win")]
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
            Assert.That(x, Is.EqualTo("entry"));

            var y = GetString(reopenedStore, "afterLastFlush");
            Assert.That(y, Is.EqualTo("mmmh")); // The index is written directly into the data file, so it is current without Flush().
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

            Assert.That(store.Contains("foo"), Is.True);
            Assert.That(store.Contains("nonexisting"), Is.False);
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

            Assert.That(GetString(store, "foo"), Is.EqualTo("bar"));
            Assert.That(GetString(store, "nonexisting"), Is.Null);
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

            Assert.That(store.GetSlice("foo", 0, 1).Length, Is.EqualTo(1));
            Assert.That(store.GetSlice("nonexisting", 0, 1), Is.Null);
        });

        [Test]
        public void OpenReadStreamFailsWhileDiskFull() => TestWithFreshStore((folder, store) =>
        {
            store.Add("foo", "bar");

            store.SimulateFullDiskOnNextResize = true;
            Assert.Throws<IOException>(() => store.Add("last drop", "to force disk full"));
            Assert.Throws<IOException>(() => store.GetStream("foo"));
            Assert.Throws<IOException>(() => store.GetStream("nonexisting"));
            store.SimulateFullDiskOnNextResize = false;

            using var s1 = store.GetStream("foo");
            Assert.That(s1, Is.Not.Null);

            using var s2 = store.GetStream("nonexisting");
            Assert.That(s2, Is.Null);
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
            Assert.That(GetString(store, "foo"), Is.Null);

            store.Remove("nonexisting");
            Assert.That(GetString(store, "nonexisting"), Is.Null);
        });

        [Test]
        public void SnapshotKeysStillWorksWhileDiskFull() => TestWithFreshStore((folder, store) =>
        {
            store.Add("foo", "bar");

            store.SimulateFullDiskOnNextResize = true;
            Assert.Throws<IOException>(() => store.Add("last drop", "to force disk full"));
            Assert.That(store.List().Count(), Is.EqualTo(1));
            store.SimulateFullDiskOnNextResize = false;

            Assert.That(store.List().Count(), Is.EqualTo(1));
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
