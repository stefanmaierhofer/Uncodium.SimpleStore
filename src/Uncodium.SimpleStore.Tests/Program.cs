using NUnit.Framework.Internal;
using System;
using System.Text;
using System.Threading;

namespace Uncodium.SimpleStore.Tests
{
    class Program
    {
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

        static void Main()
        {
            TestConcurrentCallsToFlush();

            //var dbDiskLocation = @"T:\teststore";

            //var store = new SimpleDiskStore(dbDiskLocation);
            //store.Add("foo", "bar");
            //store.Flush();

            //var readOnlyStore = SimpleDiskStore.OpenReadOnlySnapshot(dbDiskLocation);
            //Console.WriteLine(Encoding.UTF8.GetString(readOnlyStore.Get("foo")));

            //store.Dispose();

            //Console.WriteLine(Encoding.UTF8.GetString(readOnlyStore.Get("foo")));

            //readOnlyStore.Dispose();

            //var readOnlyStore2 = SimpleDiskStore.OpenReadOnlySnapshot(dbDiskLocation);
            //Console.WriteLine(Encoding.UTF8.GetString(readOnlyStore2.Get("foo")));



            //new Tests().CanAddAndGetMultiThreadedDiskStore();
        }
    }
}
