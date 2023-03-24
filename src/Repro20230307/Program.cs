using Aardvark.Data.Points;
using Aardvark.Base;
using Aardvark.Data.Points.Import;
using Aardvark.Geometry.Points;
using Uncodium.SimpleStore;
using static System.Console;

const string POINTCLOUD = @"W:\Datasets\plytest\sponza_00_mesh_sub0.ply";

const string STOREPATH = @"C:\Test\teststore\data.bin";
const string STOREPATH_WITHOUT_BIN = @"C:\Test\teststore";

Guid KEY1 = Guid.Parse("771ea77e-f868-4881-8a52-11502f2099e9");
var cts = new CancellationTokenSource();

void loglines(string[] lines)
{
    ForegroundColor = ConsoleColor.DarkGray;
    foreach (var line in lines) WriteLine($"[log] {line}");
    ResetColor();
}
var directory = new DirectoryInfo(STOREPATH_WITHOUT_BIN);

Storage? storeReadWrite = null;
Write("delete store file (y/n)? ");
switch (ReadKey().Key)
{
    case ConsoleKey.Y:
        try
        {
            if (directory.Exists)
                directory.Delete(true);
            WriteLine($"\n[0] CREATE EMPTY STORE");
            //storeReadWrite = PointCloud.OpenStore(STOREPATH, new LruDictionary<string, object>(2L << 30), loglines, false);
            storeReadWrite = new SimpleDiskStore(STOREPATH).ToPointCloudStore();
            WriteLine($"[0] created empty store");

        }
        catch (Exception e) { WriteLine($"\nFailed to delete store: {e.Message}"); }
        break;
    case ConsoleKey.N:
        break;
    default:
        WriteLine($"\nINVALID KEY -> EXIT");
        return;
}

/////////////////////////////////////////////////////////////////////////////

//Storage? storeReadWrite = null;

//WriteLine($"\n[0] CREATE EMPTY STORE");
//if (directory.Exists)
//{
//    WriteLine($"[0] store already exists");
//}
//else
//{
//    Directory.CreateDirectory(@"C:\Test\teststore");
//    //storeReadWrite = new SimpleDiskStore(STOREPATH, loglines).ToPointCloudStore();
//    storeReadWrite = PointCloud.OpenStore(STOREPATH, new LruDictionary<string,object>(2L << 30), loglines, true);
//    WriteLine($"[0] created empty store");

//    //Write($"[0] importing {POINTCLOUD} ... ");
//    //var pointset0 = PointCloud.Import(POINTCLOUD, ImportConfig.Default
//    //    .WithStorage(store0)
//    //    .WithKey(KEY1)
//    //    .WithVerbose(false)
//    //    );
//    //WriteLine($"done (key={KEY1}");
//}
WriteLine();

/////////////////////////////////////////////////////////////////////////////

Write("open store read-only (y/n)? ");
switch (ReadKey().Key)
{
    case ConsoleKey.Y:
        {
            try
            {
                WriteLine($"\n[1] OPEN STORE (READ-ONLY) and continuously read existing point cloud)");
                var storeReadOnly = PointCloud.OpenStore(STOREPATH_WITHOUT_BIN, new LruDictionary<string, object>(2L << 30), loglines, true);
                WriteLine($"[1] opened store (read-only)");
                Write("point cloud key to read (press enter for default key): ");
                var key = ReadLine();
                if (string.IsNullOrWhiteSpace(key)) key = KEY1.ToString();
                _ = Task.Run(() =>
                {
                    try
                    {
                        while (!cts.IsCancellationRequested)
                        {
                            var bs = storeReadOnly.f_get.Invoke(key);
                            if (bs != null)
                            {
                                var pointcloud = storeReadOnly.GetPointSet(key);
                                var count = 0;
                                pointcloud.Root.Value.ForEachNode(
                                    outOfCore: true,
                                    action: node => count++
                                    );
                                ForegroundColor = ConsoleColor.Blue; WriteLine($"R: {count} nodes, {pointcloud.PointCount} points"); ResetColor();
                                Thread.Sleep(2000);
                            }
                            else
                            {
                                ForegroundColor = ConsoleColor.Red; WriteLine($"Could not find Pointcloud with key {key}"); ResetColor();
                                Thread.Sleep(2000);
                            }
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
            break;
        }
    case ConsoleKey.N:
        break;
    default:
        WriteLine($": INVALID KEY -> EXIT");
        return;
}

/////////////////////////////////////////////////////////////////////////////

try
{
    _ = Task.Run(() => {

        WriteLine();
        bool print = true;
        while (!cts.IsCancellationRequested)
        {
            try
            {
                if (print) Write("import next point cloud (y/n) ... ");
                print = true;
                switch (ReadKey().Key)
                {
                    case ConsoleKey.Y:
                        {
                            WriteLine();

                            //storeReadWrite ??= PointCloud.OpenStore(STOREPATH, new LruDictionary<string, object>(2L << 30), loglines, false);
                            storeReadWrite ??= new SimpleDiskStore(STOREPATH).ToPointCloudStore();

                            var pointset = PointCloud.Import(POINTCLOUD, ImportConfig.Default
                                .WithStorage(storeReadWrite)
                                .WithRandomKey()
                                .WithVerbose(false)
                                );

                            storeReadWrite.Flush();

                            ForegroundColor = ConsoleColor.Green;
                            WriteLine($"\nimported point cloud, store size is {new FileInfo(STOREPATH).Length:N0} bytes, key={pointset.Id}");
                            ResetColor();

                            break;
                        }
                    case ConsoleKey.N:
                        Environment.Exit(0);
                        return;
                    default:
                        WriteLine();
                        print = false;
                        break;
                }
            }
            catch (Exception e)
            {
                WriteLine($"Failed to IMPORT point cloud. {e}");
                Thread.Sleep(2000);
            }
        }
    },
    cts.Token);
}
catch (Exception e)
{
    WriteLine($"Failed to OPEN store (WRITE). {e}");
}

await Task.Delay(-1);
