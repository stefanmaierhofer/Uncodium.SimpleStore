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
            var dbDiskLocation = @"/some/path/to/nowhere";
            using (var store = new SimpleDiskStore(dbDiskLocation, null))
            {
                Console.WriteLine(store.Get("23531522-e181-4e4f-985f-4927296df9b1")?.Length ?? -1);
            }
        }
    }
}
