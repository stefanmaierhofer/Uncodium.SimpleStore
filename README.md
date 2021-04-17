# Overview

SimpleStore is a low-latency key/value store optimized for blob data.

Its singular goal is to eliminate any overhead between you and your disk.

[QuickStart](#quickstart)

## Is it for me?

SimpleStore provides low-latency access to large out-of-core data structures on local storage.

It does so by efficiently managing millions of differently sized blobs in a single file without impacting your raw disk performance.

An example use case is handling of very large out-of-core datasets for real-time applications, e.g. real-time rendering of country-scale laser scans or high resolution industrial sensor data.

If you have

- large tree or graph data structures
- that don't fit into memory (out-of-core)
- with data nodes in the kilobyte to megabyte range

if you want

- low-latency random access
- raw I/O performance
- convenient single file store

if you don't need

- distributed cloud-scale storage
- query and transaction capabilities
- error detection and correction
- reliable storage for mission critical data

then it is for you!

## Tech

SimpleStore uses a single memory-mapped file to store all data and an index.
Read and write operations take buffers (byte arrays) or streams, which are directly memory-mapped to the underlying file. This basically exposes the raw IO performance of your underlying hardware - no overheads involved.

In order to achieve the lowest possible latency for random access, an in-memory index is used at runtime to eliminate any additional disk IO for indexing operations. This also means, that it makes no sense at all to use SimpleStore for storing very large numbers of really tiny blobs. The sweet spot for blob sizes is probably in the range of a few kilobytes up to hundreds of kilobytes or a few megabytes.

All operations are performed atomically. This means, that a new entry is either fully inserted or not at all. It is not possible (on the software-level) to end up with partially written blobs or corrupt index entries - even if your application crashes and exits.

Should you simple pull the power plug in the midst of inserting new entries, it is (theoretically) possible that you could end up with a valid index entry but a partially written blob. But by design SimpleStore will never lose existing data, as new entries are always appended and can never corrupt existing data. This also means, that SimpleStore is not optimized for workloads requiring constant deletion/replacement of blobs.

### Disclaimer
If your underlying file system gets corrupted (either logically or physically), then all bets are off! 
SimpleStore is not designed to handle such a situation. It does not even try to solve this problem, because any mitigation measures (redundancy, journaling, etc.) would be detrimental to performance.


## In the wild

Over the last years SimpleStore has been adopted by production quality applications (also commercially) and routinely manages datasets in the tens to hundreds of gigabytes range for real-time visualization, modeling, geometric reconstruction, and more.

Of course SimpleStore is not limited to specific application areas as it is completely agnostic of the meaning and semantics of the data it manages. Its current main application in visual computing systems is mostly a historical coincidence.

# Quickstart

add package

```shell
> dotnet add package Uncodium.SimpleStore
```

add namespace

```csharp
using Uncodium.SimpleStore;
```

open store
```csharp
using var store = new SimpleDiskStore("./mystore");
```

usage

```csharp
// write blob to store
var blob = new byte[1000000];
store.Add("mykey", blob);
```

```csharp
// read blob from store
var x = store.Get("mykey");
```

```csharp
// check if blob exists
if (store.Contains("some key")) { /* ... */ }
```

```csharp
// get size of stored blob
var size = store.GetSize("mykey");
```

```csharp
// stream blob to store
var stream = File.OpenRead("foo.jpg");
store.AddStream("my image", stream);
```

```csharp
// read blob as stream
var readstream = store.GetStream("some key");
```

```csharp
// read blob as stream (partially)
var readstream = store.GetStream("some key", offset: 123456);
```

```csharp
// read blob from store (partially)
var x = store.GetSlice("mykey", offset: 123456, length: 8192 );
```

```csharp
// force all pending changes written to disk right now
store.Flush();
```

```csharp
// enumerate all keys (including blob sizes)
foreach (var (key, size) in store.List()) Console.WriteLine($"{key} {size}");
```

```csharp
// remove blob
store.Remove("some key");
```