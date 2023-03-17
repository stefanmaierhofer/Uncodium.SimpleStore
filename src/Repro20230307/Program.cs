using Aardvark.Data.Points;
using Aardvark.Geometry.Points;
using Uncodium.SimpleStore;
using static System.Console;

const string POINTCLOUD = @"W:\Datasets\plytest\sponza_00_mesh_sub0.ply";

const string STOREPATH = @"./teststore.dur";

Guid KEY1 = Guid.Parse("771ea77e-f868-4881-8a52-11502f2099e9");
Guid KEY2 = Guid.Parse("653d7bcb-2511-40b8-9709-13efdc34736a");
var cts = new CancellationTokenSource();

WriteLine("delete store file (y/N)?");
switch (ReadKey().Key)
{
    case ConsoleKey.Y:
        try { File.Delete(STOREPATH); }
        catch (Exception e) { WriteLine($"Failed to delete store: {e.Message}"); }
        break;
    case ConsoleKey.N:
        break;
    default:
        WriteLine($": INVALID KEY -> EXIT");
        return;
}

/////////////////////////////////////////////////////////////////////////////
WriteLine($"[0] CREATE EMPTY STORE AND IMPORT POINT CLOUD");
if (File.Exists(STOREPATH))
{
    WriteLine($"[0] store already exists (no import necessary)");
}
else
{
    Directory.CreateDirectory(@"T:\tmp");
    using var store0 = new SimpleDiskStore(STOREPATH).ToPointCloudStore();
    WriteLine($"[0] created empty store");

    Write($"[0] importing {POINTCLOUD} ... ");
    var pointset0 = PointCloud.Import(POINTCLOUD, ImportConfig.Default
        .WithStorage(store0)
        .WithKey(KEY1)
        .WithVerbose(false)
        );
    WriteLine($"done");
}
WriteLine();

/////////////////////////////////////////////////////////////////////////////
try 
{
    WriteLine($"[1] OPEN STORE (READ-ONLY) and continuously read existing point cloud)");
    var storeReadOnly = SimpleDiskStore
        .OpenReadOnlySnapshot(STOREPATH)
        .ToPointCloudStore(cache: null) // don't use a cache to force file access
        ;
    WriteLine($"[1] opened store (read-only)");
    _ = Task.Run(() =>
    {
        try
        {
            while (!cts.IsCancellationRequested)
            {
                //var pointcloud = storeReadOnly.GetPointSet(KEY1);
                //var count = 0;
                //pointcloud.Root.Value.ForEachNode(
                //    outOfCore: true,
                //    action: node => count++
                //    );
                Task.Delay(500).Wait();
                ForegroundColor = ConsoleColor.Blue; Write("R"); ResetColor();
            }
        }
        catch (Exception e)
        {
            WriteLine($"Failed to READ point cloud. {e.Message}");
        }
    },
    cts.Token);
}
catch (Exception e) 
{
    WriteLine($"Failed to OPEN store (READ-ONLY). {e.Message}");
}

WriteLine();
WriteLine("============================================");
WriteLine("press any key to START POINTCLOUD IMPORT ...");
WriteLine("============================================");
ReadKey();

/////////////////////////////////////////////////////////////////////////////
try
{
    WriteLine();
    WriteLine($"[2] OPEN STORE (WRITE) and continuously import same point cloud");
    var storeReadWrite = new SimpleDiskStore(STOREPATH)
        .ToPointCloudStore(cache: null) // don't use a cache to force file access
        ;
    WriteLine($"[2] opened store (read/write)");
    _ = Task.Run(() =>
    {
        try 
        {
            while (!cts.IsCancellationRequested)
            {
                var pointset = PointCloud.Import(POINTCLOUD, ImportConfig.Default
                    .WithStorage(storeReadWrite)
                    .WithKey(KEY2)
                    .WithVerbose(false)
                    );

                ForegroundColor = ConsoleColor.Green;
                WriteLine($"\nimported point cloud, store size is {new FileInfo(STOREPATH).Length:N0} bytes");
                ResetColor();
            }
        }
        catch (Exception e)
        {
            WriteLine($"Failed to IMPORT point cloud. {e.Message}");
        }
    },
    cts.Token);
}
catch (Exception e) 
{
    WriteLine($"Failed to OPEN store (WRITE). {e.Message}");
}

WriteLine();
WriteLine("=========================");
WriteLine("press any key to stop ...");
WriteLine("=========================");
ReadKey();
cts.Cancel();