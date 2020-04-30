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
            using (var store = new SimpleDiskStore(dbDiskLocation))
            {
                using (var readOnlyStore = new SimpleDiskStore(dbDiskLocation, readOnly: true))
                {
                    store.Add("foo", "bar");

                    var v = readOnlyStore.Get("foo");
                    Console.WriteLine(v);
                }
            }
        }
    }
}
