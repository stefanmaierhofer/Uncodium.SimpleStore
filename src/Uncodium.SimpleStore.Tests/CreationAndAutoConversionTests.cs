using NUnit.Framework;
using System;
using System.IO;

namespace Uncodium.SimpleStore.Tests
{
    [TestFixture]
    public class CreationAndAutoConversionTests
    {
        /// <summary>
        /// Creates old store in tmp folder and returns path to this folder.
        /// </summary>
        private static string CreateOldStore()
        {
            const int count = 1000;
            var path = Path.Combine(Path.GetTempPath(), $"oldstore_{Guid.NewGuid()}");

            Directory.CreateDirectory(path);
            using var bwIndex = new BinaryWriter(File.OpenWrite(Path.Combine(path, "index.bin")));
            using var bwData  = new BinaryWriter(File.OpenWrite(Path.Combine(path, "data.bin")));

            bwData.Write(0L);

            var p = 8L;
            var r = new Random();
            bwIndex.Write(count); // entry count
            for (var i = 0; i < count; i++)
            {
                var buffer = new byte[r.Next(16 * 1024)];
                r.NextBytes(buffer);

                bwData.Write(buffer);

                bwIndex.Write($"key-{i}");
                bwIndex.Write(p);
                bwIndex.Write(buffer.Length);

                p += buffer.Length;
            }

            bwData.BaseStream.Position = 0L;
            bwData.Write(p);

            return path;
        }

        private static void DeleteOldStore(string path)
        {
            Directory.Delete(path, true);
        }

        [Test]
        public void CanCreateTestStoreInOldFormat()
        {
            var path = CreateOldStore();
            try
            {
                Assert.True(Directory.Exists(path));
                Assert.True(File.Exists(Path.Combine(path, "data.bin")));
                Assert.True(File.Exists(Path.Combine(path, "index.bin")));
                Assert.True(SimpleDiskStore.GetStoreLayout(path) == SimpleDiskStore.StoreLayout.FolderWithStandaloneDataAndIndexFiles);
            }
            finally
            {
                DeleteOldStore(path);
            }

            Assert.False(Directory.Exists(path));
        }

        [Test]
        public void OpeningDataFileInOldStyleStoreDirectlyWillFail()
        {
            var path = CreateOldStore();

            try
            {
                Assert.True(SimpleDiskStore.GetStoreLayout(path) == SimpleDiskStore.StoreLayout.FolderWithStandaloneDataAndIndexFiles);

                var oldStyleDataFile = Path.Combine(path, "data.bin");
                Assert.True(File.Exists(oldStyleDataFile));
                Assert.Throws<Exception>(() => new SimpleDiskStore(oldStyleDataFile));
            }
            finally
            {
                DeleteOldStore(path);
            }
        }

        [Test]
        public void CanAutoConvertOldStore()
        {
            var path = CreateOldStore();

            try
            {
                Assert.True(SimpleDiskStore.GetStoreLayout(path) == SimpleDiskStore.StoreLayout.FolderWithStandaloneDataAndIndexFiles);

                using var store = new SimpleDiskStore(path);

                Assert.True(SimpleDiskStore.GetStoreLayout(path) == SimpleDiskStore.StoreLayout.FolderWithMergedDataAndIndexFile);

            }
            finally
            {
                DeleteOldStore(path);
            }
        }

        [Test]
        public void CanOpenAutoConvertedOldStore()
        {
            var path = CreateOldStore();
            Assert.True(Directory.Exists(path));
            Assert.False(File.Exists(path));

            try
            {
                Assert.True(SimpleDiskStore.GetStoreLayout(path) == SimpleDiskStore.StoreLayout.FolderWithStandaloneDataAndIndexFiles);

                var store = new SimpleDiskStore(path);
                store.Add("foo", "bar");
                store.Dispose();

                Assert.True(SimpleDiskStore.GetStoreLayout(path) == SimpleDiskStore.StoreLayout.FolderWithMergedDataAndIndexFile);

                using var store2 = new SimpleDiskStore(path);
                Assert.True(store2.Contains("foo"));
            }
            finally
            {
                DeleteOldStore(path);
            }
        }

        [Test]
        public void OpendingDataFileOfNonConvertedOldStyleStoreDirectlyWillFail()
        {
            var path = CreateOldStore();
            Assert.True(Directory.Exists(path));
            Assert.False(File.Exists(path));

            try
            {
                var oldStyleDataFile = Path.Combine(path, "data.bin");
                Assert.True(File.Exists(oldStyleDataFile));

                Assert.Throws<Exception>(() => new SimpleDiskStore(oldStyleDataFile));
            }
            finally
            {
                DeleteOldStore(path);
            }
        }

        [Test]
        public void CanOpenAutoConvertedDataFileDirectly()
        {
            var path = CreateOldStore();
            Assert.True(Directory.Exists(path));
            Assert.False(File.Exists(path));

            try
            {
                Assert.True(SimpleDiskStore.GetStoreLayout(path) == SimpleDiskStore.StoreLayout.FolderWithStandaloneDataAndIndexFiles);

                var store = new SimpleDiskStore(path);
                store.Add("foo", "bar");
                store.Dispose();

                Assert.True(SimpleDiskStore.GetStoreLayout(path) == SimpleDiskStore.StoreLayout.FolderWithMergedDataAndIndexFile);

                var dataFileName = Path.Combine(path, "data.bin");
                Assert.True(File.Exists(dataFileName));
                using var store2 = new SimpleDiskStore(dataFileName);
                Assert.True(store2.Contains("foo"));
            }
            finally
            {
                DeleteOldStore(path);
            }
        }

        [Test]
        public void CanCreateNewSingleFileStore_ContainingFolderExists()
        {
            var tmpFolder = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}");
            Directory.CreateDirectory(tmpFolder);

            try
            {
                var filename = Path.Combine(tmpFolder, "mystore.uds");
                using var store = new SimpleDiskStore(filename);

                Assert.True(SimpleDiskStore.GetStoreLayout(filename) == SimpleDiskStore.StoreLayout.SingleFile);
                Assert.True(File.Exists(filename + ".log"));
            }
            finally
            {
                Directory.Delete(tmpFolder, true);
            }
        }

        [Test]
        public void CanCreateNewSingleFileStore_ContainingFolderExists_CustomExtension()
        {
            var tmpFolder = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}");
            Directory.CreateDirectory(tmpFolder);

            try
            {
                var filename = Path.Combine(tmpFolder, "mystore.custom.data.extension");
                using var store = new SimpleDiskStore(filename);

                Assert.True(SimpleDiskStore.GetStoreLayout(filename) == SimpleDiskStore.StoreLayout.SingleFile);
                Assert.True(File.Exists(filename + ".log"));
            }
            finally
            {
                Directory.Delete(tmpFolder, true);
            }
        }



        [Test]
        public void CanOpenStoreWithOldStyleNameViaEnclosingDirectory()
        {
            var tmpFolder = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}");
            Directory.CreateDirectory(tmpFolder);

            try
            {
                var filename = Path.Combine(tmpFolder, "data.bin");
                var store = new SimpleDiskStore(filename);
                store.Add("foo", "bar");
                store.Dispose();

                using var store2 = new SimpleDiskStore(tmpFolder);
                Assert.True(store2.Contains("foo"));
            }
            finally
            {
                Directory.Delete(tmpFolder, true);
            }
        }

        [Test]
        public void CanCreateNewSingleFileStore_ContainingFolderExists_NoExtension()
        {
            var tmpFolder = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}");
            Directory.CreateDirectory(tmpFolder);

            try
            {
                var filename = Path.Combine(tmpFolder, "mystore");
                using var store = new SimpleDiskStore(filename);

                Assert.True(SimpleDiskStore.GetStoreLayout(filename) == SimpleDiskStore.StoreLayout.SingleFile);
                Assert.True(File.Exists(filename + ".log"));
            }
            finally
            {
                Directory.Delete(tmpFolder, true);
            }
        }

        [Test]
        public void CanCreateNewSingleFileStore_ContainingFolderDoesNotExist()
        {
            var tmpFolder = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}");

            try
            {
                var filename = Path.Combine(tmpFolder, "mystore");
                using var store = new SimpleDiskStore(filename);

                Assert.True(SimpleDiskStore.GetStoreLayout(filename) == SimpleDiskStore.StoreLayout.SingleFile);
                Assert.True(File.Exists(filename));
                Assert.True(File.Exists(filename + ".log"));
            }
            finally
            {
                Directory.Delete(tmpFolder, true);
            }
        }

        [Test]
        public void CanOpenNewlyCreatedSingleFileStore()
        {
            var tmpFolder = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}");

            try
            {
                var filename = Path.Combine(tmpFolder, "mystore.uds");

                // create new store, add an entry, and close again
                var store = new SimpleDiskStore(filename);
                store.Add("foo", "bar");
                store.Dispose();

                // (re)open store
                using var store2 = new SimpleDiskStore(filename);
                Assert.True(store2.Contains("foo"));
            }
            finally
            {
                Directory.Delete(tmpFolder, true);
            }
        }

        [Test]
        public void CreateStoreFailsIfPathIsAnEmptyDirectory()
        {
            var folder = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}");
            Directory.CreateDirectory(folder);

            try
            {
                Assert.True(Directory.Exists(folder));
                Assert.Throws<Exception>(() => new SimpleDiskStore(folder));
            }
            finally
            {
                Directory.Delete(folder, true);
            }
        }

        [Test]
        public void CreateStoreFailsIfPathIsAnUnknownFile()
        {
            // create tmp folder
            var folder = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}");
            Directory.CreateDirectory(folder);

            // create file with invalid content
            var path = Path.Combine(folder, "data.bin");
            File.WriteAllText(path, "this is not a store");

            try
            {
                Assert.Throws<Exception>(() => new SimpleDiskStore(path));
            }
            finally
            {
                Directory.Delete(folder, true);
            }
        }
    }
}
