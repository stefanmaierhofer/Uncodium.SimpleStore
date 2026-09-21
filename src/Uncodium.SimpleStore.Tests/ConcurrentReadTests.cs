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
    /// <summary>
    /// Readers run concurrently under a shared lock while a writer appends (and forces data file resizes).
    /// </summary>
    [TestFixture]
    public class ConcurrentReadTests
    {
        private static byte[] ValueFor(string key)
        {
            var bytes = Encoding.UTF8.GetBytes(key);
            var result = new byte[100];
            for (var i = 0; i < result.Length; i++) result[i] = bytes[i % bytes.Length];
            return result;
        }

        [Test]
        public void ConcurrentReadersWithWriter_SeeConsistentValues()
        {
            var path = Path.Combine(Path.GetTempPath(), $"simple-store-concurrent-reads-{Guid.NewGuid():N}");
            try
            {
                using var store = new SimpleDiskStore(path);

                const int seededCount = 1000;
                var seededKeys = Enumerable.Range(0, seededCount).Select(i => $"k{i}").ToArray();
                foreach (var k in seededKeys) store.Add(k, ValueFor(k));

                var reservedBefore = store.GetReservedBytes();

                // writer: appends enough data to force several resizes of the data file
                const int writerCount = 12_000;
                var writerDone = 0;
                var writer = Task.Run(() =>
                {
                    var data = new byte[8 * 1024];
                    for (var i = 0; i < writerCount; i++) store.Add($"w{i}", data);
                    Interlocked.Exchange(ref writerDone, 1);
                });

                // readers: read seeded keys (and probe writer keys) until the writer is done
                const int readerCount = 8;
                var reads = new long[readerCount];
                var readers = Enumerable.Range(0, readerCount).Select(r => Task.Run(() =>
                {
                    var rnd = new Random(r);
                    while (Volatile.Read(ref writerDone) == 0)
                    {
                        var key = seededKeys[rnd.Next(seededCount)];
                        var value = store.Get(key);
                        Assert.That(value, Is.Not.Null, key);
                        Assert.That(value, Is.EqualTo(ValueFor(key)), key);
                        Assert.That(store.Contains(key), Is.True, key);
                        Assert.That(store.GetSize(key), Is.EqualTo(100), key);

                        var slice = store.GetSlice(key, 10, 20);
                        Assert.That(slice, Is.EqualTo(ValueFor(key).Skip(10).Take(20).ToArray()), key);

                        // a writer key either does not exist yet or is complete
                        var w = $"w{rnd.Next(writerCount)}";
                        var wv = store.Get(w);
                        if (wv != null) Assert.That(wv.Length, Is.EqualTo(8 * 1024), w);

                        reads[r]++;
                    }
                })).ToArray();

                Task.WaitAll(readers.Append(writer).ToArray());

                Assert.That(store.GetReservedBytes(), Is.GreaterThan(reservedBefore), "writer should have forced at least one resize");
                Assert.That(reads.Sum(), Is.GreaterThan(readerCount * 10), "readers should have made progress");

                foreach (var k in seededKeys) Assert.That(store.Get(k), Is.EqualTo(ValueFor(k)));
                Assert.That(Enumerable.Range(0, writerCount).All(i => store.Contains($"w{i}")), Is.True);
                Assert.That(store.List().Count(), Is.EqualTo(seededCount + writerCount));
            }
            finally
            {
                try { Directory.Delete(path, recursive: true); } catch { }
            }
        }

        [Test]
        public void ReadersOverlap()
        {
            // With a shared lock, N readers on one key finish in roughly the time of one reader, not N.
            var path = Path.Combine(Path.GetTempPath(), $"simple-store-concurrent-reads-{Guid.NewGuid():N}");
            try
            {
                using var store = new SimpleDiskStore(path);
                var data = new byte[64 * 1024 * 1024];
                new Random(0).NextBytes(data);
                store.Add("big", data);
                store.Get("big"); // warm

                double Time(int threads)
                {
                    using var barrier = new Barrier(threads);
                    var sw = System.Diagnostics.Stopwatch.StartNew();
                    var ts = Enumerable.Range(0, threads).Select(_ => Task.Factory.StartNew(() =>
                    {
                        barrier.SignalAndWait();
                        for (var i = 0; i < 4; i++) Assert.That(store.Get("big").Length, Is.EqualTo(data.Length));
                    }, TaskCreationOptions.LongRunning)).ToArray();
                    Task.WaitAll(ts);
                    return sw.Elapsed.TotalSeconds;
                }

                Time(1);
                var t1 = Math.Min(Time(1), Time(1));
                var t8 = Math.Min(Time(8), Time(8));
                TestContext.Out.WriteLine($"1 reader: {t1:0.000} s, 8 readers: {t8:0.000} s (serialized would be ~{8 * t1:0.000} s)");
                Assert.That(t8, Is.LessThan(8 * t1 * 0.75), "readers do not overlap");
            }
            finally
            {
                try { Directory.Delete(path, recursive: true); } catch { }
            }
        }
    }
}
