using NUnit.Framework.Constraints;
using NUnit.Framework.Internal;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
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

