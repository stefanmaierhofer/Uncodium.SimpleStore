using NUnit.Framework.Constraints;
using NUnit.Framework.Internal;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Uncodium.SimpleStore;

#pragma warning disable CS8321

static void TestConcurrentCallsToFlush()
{
    var dbDiskLocation = @"T:\teststore";
    Console.WriteLine("open store");
    using var store = new SimpleDiskStore(dbDiskLocation, lines =>
    {
        Console.ForegroundColor = ConsoleColor.Cyan;
        foreach (var line in lines) Console.WriteLine(line);
        Console.ResetColor();
    });
    Console.WriteLine("add many entries");
    for (var i = 0; i < 1_000_000; i++) store.Add(Guid.NewGuid().ToString(), new[] { (byte)42 });
            

    var go = new ManualResetEventSlim();

    var t0 = new Thread(() =>
    {
        go.Wait();
        for (var i = 0; i < 5; i++)
        {
            Console.WriteLine("flush0 begin");
            store.Flush();
            Console.WriteLine("flush0 end");
        }

    });
    t0.Start();

    var t1 = new Thread(() =>
    {
        go.Wait();
        for (var i = 0; i < 5; i++)
        {
            Console.WriteLine("flush1 begin");
            store.Flush();
            Console.WriteLine("flush1 end");
        }
    });
    t1.Start();

    Console.WriteLine("press enter to flush concurrently ...");
    Console.ReadLine();
    go.Set();

    for (var i = 0; i < 1_000_000; i++) store.Add(Guid.NewGuid().ToString(), new[] { (byte)42 });

    store.Dispose();

    store.Flush();

    store.Dispose();
}

static void TestThroughput()
{
    var totalBytes = 16L * 1024 * 1024 * 1024;

    var sw = new Stopwatch();
    var rand = new Random();

    //var f = File.OpenWrite(@"T:\tmp\throughputtest0");

    var store = new SimpleDiskStore(@"T:\tmp\throutputtest1");
    var totalBytesLeft = totalBytes;
    var totalEntries = 0;
    sw.Restart();
    while (totalBytesLeft > 0L)
    {
        var r = rand.NextDouble();
        var size = (int)(r * r * r * 1024 * 1024);
        if (size > totalBytesLeft) size = (int)totalBytesLeft;
        var buffer = new byte[size];
        var key = Guid.NewGuid().ToString();
        store.Add(key, buffer);
        //f.Write(buffer, 0, size);
        totalBytesLeft -= buffer.Length;
        totalEntries++;
    }
    //f.Flush();
    store.Flush();
    sw.Stop();
    Console.WriteLine($"elapsed time : {sw.Elapsed,20}");
    Console.WriteLine($"wrote entries: {totalEntries,20:N0}");
    Console.WriteLine($"      bytes  : {totalBytes,20:N0}");
    Console.WriteLine($"      bytes/s: {totalBytes/sw.Elapsed.TotalSeconds,20:N0}");
}

static void TestAutoConversion()
{
    var store = new SimpleDiskStore(@"T:\tmp\1190_31_test_Frizzo.e57_5.0.24");
    var i = 1L;
    var keys = store.List().Select(x => x.key).ToArray();
    foreach (var k in keys)
    {
        var buffer = store.Get(k);
        Console.WriteLine($"[{i,16:N0}/{keys.Length:N0}] {k,-20} {buffer.Length,16:N0} bytes");
        ++i;
    }
}

static void Quickstart()
{
    using var store = new SimpleDiskStore("./mystore");

    var blob = new byte[1000000];
    store.Add("mykey", blob);

    var x = store.Get("mykey");

    var stream = File.OpenRead("foo.jpg");
    store.AddStream("my image", stream);

    var readstream = store.GetStream("my image");

    foreach (var (key, size) in store.List()) Console.WriteLine($"{key} {size}");

}

static void ExtractStoreToFolder(string storeToExtract, string targetFolder)
{
    if (Directory.Exists(targetFolder)) Directory.Delete(targetFolder, true);

    using var source = new SimpleDiskStore(storeToExtract);
    using var target = new SimpleFolderStore(targetFolder);

    var entries = source.List().ToArray();
    var keys = entries.Select(x => x.key).ToArray();
    Console.WriteLine($"Extracting {keys.Length:N0} entries to {targetFolder}.");
    var t0 = DateTimeOffset.Now;
    var timestampLastProgress = t0;
    for (var i = 0; i < keys.Length; i++)
    {
        var key = keys[i];
        var buffer = source.Get(key);
        target.Add(key, buffer);

        if ((DateTimeOffset.Now - timestampLastProgress).TotalSeconds > 1.0)
        {
            timestampLastProgress = DateTimeOffset.Now;
            Console.WriteLine($"{DateTime.Now} | {i + 1,9:N0}/{keys.Length:N0} | {100.0 * (i + 1) / keys.Length:000.00}%");
        }
    }
    Console.WriteLine($"{DateTime.Now} | {keys.Length,9:N0}/{keys.Length:N0} | {100.0:000.00}%");
}

static void CompareFolders(string folder1, string folder2)
{
    var files1 = Directory.GetFiles(folder1).OrderBy(x => x).ToArray();
    var files2 = Directory.GetFiles(folder2).OrderBy(x => x).ToArray();
    if (files1.Length != files2.Length) throw new Exception($"Different number of files ({files1.Length} != {files2.Length}).");
    for (var i = 0; i < files1.Length; i++)
    {
        if (Path.GetFileName(files1[i]) != Path.GetFileName(files2[i])) throw new Exception($"File name mismatch: {Path.GetFileName(files1[i])} != {Path.GetFileName(files2[i])}");

        Console.WriteLine($"{Path.GetFileName(files1[i])}");

        var buffer1 = File.ReadAllBytes(files1[i]);
        var buffer2 = File.ReadAllBytes(files2[i]);
        if (buffer1.Length != buffer2.Length) throw new Exception($"File size mismatch: {Path.GetFileName(files1[i])},  {buffer1.Length} != {buffer2.Length}");
        for (var j = 0; j < buffer1.Length; j++)
        {
            if (buffer1[j] != buffer2[j])
            {
                Console.WriteLine(
                    $"\nFile content mismatch: {Path.GetFileName(files1[i])}, size = {buffer1.Length}, offset {j}, {buffer1[j]} != {buffer2[j]}"
                    );

                Console.WriteLine("Press <enter> to show diff.");
                Console.ReadLine();

                for (var k = 0; k < buffer1.Length; k++)
                {
                    var c1 = buffer1[k];
                    var c2 = buffer2[k];

                    if (c1 != c2)
                    {
                        Console.BackgroundColor = ConsoleColor.DarkRed;
                        Console.ForegroundColor = ConsoleColor.Yellow;
                    }
                    Console.WriteLine($"[{k,4}] {c1:X2} {c2:X2}");
                    if (c1 != c2) Console.ResetColor();
                }

                break;
            }
        }
    }
}

static async Task TestSimpleAzureBlobStore(string sas)
{
    var store = new SimpleAzureBlobStore(sas);

    //// list
    //long i = 0;
    //foreach (var (key, size) in store.List())
    //{
    //    Console.WriteLine($"[{++i}] {key} {size,16:N0}");
    //}

    var buffer1 = await store.GetAsync("2eda3f92-1b33-46e2-b2a5-0f490347621a");
    var buffer2 = await store.GetSliceAsync("2eda3f92-1b33-46e2-b2a5-0f490347621a", 10, 20);
}


TestSimpleAzureBlobStore("your sas here").Wait();


//ExtractStoreToFolder(@"T:\Vgm\Data\20210429_adorjan_store2.1.10", @"E:\tmp\20210429_adorjan_store2.1.10_new");
//CompareFolders(@"E:\tmp\20210429_adorjan_store2.1.10_old", @"E:\tmp\20210429_adorjan_store2.1.10_new");

//using var store = new SimpleDiskStore(@"E:\tmp\foo");


//TestAutoConversion();

//TestThroughput();

//Console.WriteLine(Encoding.UTF8.GetBytes($"{DateTimeOffset.Now:O}").Length);

//var dbDiskLocation = @"T:\teststore";
//var store = new SimpleDiskStore(dbDiskLocation);
//try
//{
//    store.Add("foo1", "bar");
//    store.Flush();
//    store.SimulateFullDiskOnNextResize = true;
//    store.Add("foo2", "bar");
//    store.Flush();
//}
//catch (System.IO.IOException e)
//{
//    store.SimulateFullDiskOnNextResize = false;
//    Console.WriteLine($"The application caught an exception: {e.Message}.");
//    Console.WriteLine("Trying to use store after exception.");
//    string GetString(string key) => Encoding.UTF8.GetString(store.Get(key));
//    Console.WriteLine($"Read from store: foo1 -> {GetString("foo1")}");
//    store.Add("foo3", "bar3");
//    Console.WriteLine($"Add to store   : foo3 -> foo3");
//    Console.WriteLine($"Read from store: foo3 -> {GetString("foo3")}");
//}

//TestConcurrentCallsToFlush();

//var readOnlyStore = SimpleDiskStore.OpenReadOnlySnapshot(dbDiskLocation);
//Console.WriteLine(Encoding.UTF8.GetString(readOnlyStore.Get("foo")));

//store.Dispose();

//Console.WriteLine(Encoding.UTF8.GetString(readOnlyStore.Get("foo")));

//readOnlyStore.Dispose();

//var readOnlyStore2 = SimpleDiskStore.OpenReadOnlySnapshot(dbDiskLocation);
//Console.WriteLine(Encoding.UTF8.GetString(readOnlyStore2.Get("foo")));



//new Tests().CanAddAndGetMultiThreadedDiskStore();

