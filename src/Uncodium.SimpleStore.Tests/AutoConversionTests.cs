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
    public class AutoConversionTests
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
    }
}
