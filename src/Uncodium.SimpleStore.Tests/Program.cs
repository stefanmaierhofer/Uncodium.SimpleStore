﻿using NUnit.Framework.Internal;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing.Printing;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace Uncodium.SimpleStore.Tests
{
    class Program
    {
        struct Entry
        {
            public int hashCode;    // Lower 31 bits of hash code, -1 if unused
            public int next;        // Index of next entry, -1 if last
            public string key;           // Key of entry
            public long value;         // Value of entry
            public int value2;
        }

        static bool IsPrime(int x)
        {
            var imax = (int)Math.Sqrt(x) + 1;
            for (var i = 3; i <= imax; i++) if (x % i == 0) return false;
            return true;
        }

        static void ExtractStoreToFolder(string storeToExtract, string targetFolder)
        {
            if (Directory.Exists(targetFolder)) Directory.Delete(targetFolder, true);

            using var source = new SimpleDiskStore(storeToExtract);
            using var target = new SimpleFolderStore(targetFolder);

            var keys = source.SnapshotKeys();
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
                    Console.WriteLine($"{DateTime.Now} | {i+1,9:N0}/{keys.Length:N0} | {100.0*(i+1)/keys.Length:000.00}%");
                }
            }
            Console.WriteLine($"{DateTime.Now} | {keys.Length,9:N0}/{keys.Length:N0} | {100.0:000.00}%");
        }

        static void Main()
        {
            ExtractStoreToFolder(@"T:\Vgm\Data\20210429_adorjan_store2.1.10", @"E:\tmp\20210429_adorjan_store2.1.10_old");

            //var foo = new int[536870912-100];
            //Console.WriteLine(IsPrime(44157247));
            //Console.WriteLine(IsPrime(88314497));
            //var newSize = 44157247 * 2;
            //while (!IsPrime(++newSize)) ;
            //Console.WriteLine(newSize);

            //Console.WriteLine($"{ Marshal.SizeOf<Entry>():N0}");

            //Console.WriteLine("START");
            //var dbDiskLocation = @"W:\teststore";
            //var store = new SimpleDiskStore(dbDiskLocation);
            //Console.WriteLine("store opened");
            //var i = 0L;
            //var sw = new Stopwatch(); sw.Start();
            ////var index = new Dictionary<string, (long, int)>();
            //try
            //{
            //    while (true)
            //    {
            //        if (++i % 1000000 == 0) Console.WriteLine($"[{sw.Elapsed}] {i,20:N0} entries");
            //        if (i % 10000000 == 0) store.Flush();
            //        store.Add(Guid.NewGuid().ToString(), new byte[1]);
            //        //index.Add(Guid.NewGuid().ToString(), (0, 0));
            //    }
            //}
            //catch (Exception e)
            //{
            //    Console.WriteLine(e);
            //    Console.WriteLine($"[{sw.Elapsed}] {i,20:N0} entries");
            //}

            //TestConcurrentCallsToFlush();

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
