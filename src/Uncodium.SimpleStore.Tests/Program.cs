using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Uncodium.SimpleStore.Tests
{
    class Program
    {
        static void Main()
        {
            var dbDiskLocation = @"T:\teststore";

            var store = new SimpleDiskStore(dbDiskLocation);
            store.Add("foo", "bar");
            store.Flush();

            var readOnlyStore = SimpleDiskStore.OpenReadOnlySnapshot(dbDiskLocation);
            Console.WriteLine(Encoding.UTF8.GetString(readOnlyStore.Get("foo")));

            store.Dispose();

            Console.WriteLine(Encoding.UTF8.GetString(readOnlyStore.Get("foo")));

            readOnlyStore.Dispose();

            var readOnlyStore2 = SimpleDiskStore.OpenReadOnlySnapshot(dbDiskLocation);
            Console.WriteLine(Encoding.UTF8.GetString(readOnlyStore2.Get("foo")));
        }
    }
}
