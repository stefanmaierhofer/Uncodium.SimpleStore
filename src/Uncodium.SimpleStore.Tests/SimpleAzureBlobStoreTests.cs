using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Uncodium.SimpleStore.Tests;

[TestFixture]
public class SimpleAzureBlobStoreTests
{
    private const string SasEnvironmentVariable = "UNCODIUM_SIMPLESTORE_AZURE_SAS";
    private const string FakeSasUri = "https://account.blob.core.windows.net/container?not-a-secret";
    private static readonly string RunPrefix = $"tests/{Guid.NewGuid():N}";

    private static string RequireSas()
    {
        var sas = Environment.GetEnvironmentVariable(SasEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(sas))
        {
            Assert.Ignore($"Set {SasEnvironmentVariable} to a container SAS URI to run Azure integration tests.");
        }

        return sas!;
    }

    private SimpleAzureBlobStore CreateStore([CallerMemberName] string testName = "")
        => CreateStoreFor(testName);

    private static SimpleAzureBlobStore CreateStoreFor(string prefix)
        => new(RequireSas(), $"{RunPrefix}/{prefix}");

    private static string GetString(SimpleAzureBlobStore store, string key)
        => Encoding.UTF8.GetString(store.Get(key));

    private static async Task<string> GetStringAsync(SimpleAzureBlobStore store, string key)
        => Encoding.UTF8.GetString(await store.GetAsync(key));

    private static void AssertEntries(
        IEnumerable<(string key, long size)> actual,
        params (string key, long size)[] expected)
        => Assert.That(actual, Is.EquivalentTo(expected));

    [Test]
    public void CanCreateStoreFromSas()
    {
        using var store = new SimpleAzureBlobStore(FakeSasUri);
    }

    [Test]
    public void CanCreateStoreFromSasAndPrefix()
    {
        using var store = new SimpleAzureBlobStore(FakeSasUri, "tests/prefix");
    }

    [Test]
    public void CanAddGetRemoveExists()
    {
        const string key = "content";
        using var store = CreateStore();

        store.Add(key, "my content");
        try
        {
            Assert.That(GetString(store, key), Is.EqualTo("my content"));
        }
        finally
        {
            store.Remove(key);
        }

        Assert.That(store.Get(key), Is.Null);
    }

    [Test]
    public async Task CanAddGetRemoveExistsAsync()
    {
        const string key = "content";
        using var store = CreateStore();

        await store.AddAsync(key, "my content");
        try
        {
            Assert.That(await GetStringAsync(store, key), Is.EqualTo("my content"));
        }
        finally
        {
            await store.RemoveAsync(key);
        }

        Assert.That(await store.GetAsync(key), Is.Null);
    }

    [Test]
    public void GetWriteStream()
    {
        const string key = "write-stream";
        using var store = CreateStore();

        try
        {
            using (var stream = store.GetWriteStream(key))
            {
                new MemoryStream(Encoding.UTF8.GetBytes("my stream content")).CopyTo(stream);
            }

            Assert.That(GetString(store, key), Is.EqualTo("my stream content"));
        }
        finally
        {
            store.Remove(key);
        }

        Assert.That(store.Get(key), Is.Null);
    }

    [Test]
    public async Task GetWriteStreamAsync()
    {
        const string key = "write-stream";
        using var store = CreateStore();

        try
        {
            using (var stream = await store.GetWriteStreamAsync(key))
            {
                await new MemoryStream(Encoding.UTF8.GetBytes("my stream content")).CopyToAsync(stream);
            }

            Assert.That(await GetStringAsync(store, key), Is.EqualTo("my stream content"));
        }
        finally
        {
            await store.RemoveAsync(key);
        }

        Assert.That(await store.GetAsync(key), Is.Null);
    }

    [Test]
    public void GetReadStream()
    {
        const string key = "read-stream";
        using var store = CreateStore();

        try
        {
            using (var stream = store.GetWriteStream(key))
            {
                new MemoryStream(Encoding.UTF8.GetBytes("my stream content")).CopyTo(stream);
            }

            using var readStream = store.GetStream(key);
            Assert.That(readStream, Is.Not.Null);
            using var result = new MemoryStream();
            readStream.CopyTo(result);
            Assert.That(Encoding.UTF8.GetString(result.ToArray()), Is.EqualTo("my stream content"));
        }
        finally
        {
            store.Remove(key);
        }

        Assert.That(store.Get(key), Is.Null);
    }

    [Test]
    public async Task GetReadStreamAsync()
    {
        const string key = "read-stream";
        using var store = CreateStore();

        try
        {
            using (var stream = await store.GetWriteStreamAsync(key))
            {
                await new MemoryStream(Encoding.UTF8.GetBytes("my stream content")).CopyToAsync(stream);
            }

            using var readStream = await store.GetStreamAsync(key);
            Assert.That(readStream, Is.Not.Null);
            using var result = new MemoryStream();
            await readStream.CopyToAsync(result);
            Assert.That(Encoding.UTF8.GetString(result.ToArray()), Is.EqualTo("my stream content"));
        }
        finally
        {
            await store.RemoveAsync(key);
        }

        Assert.That(await store.GetAsync(key), Is.Null);
    }

    [Test]
    public void GetSlice()
    {
        const string key = "slice";
        using var store = CreateStore();

        try
        {
            store.Add(key, "0123456789");
            var buffer = store.GetSlice(key, 1, 4);
            Assert.That(buffer, Is.Not.Null);
            Assert.That(Encoding.UTF8.GetString(buffer), Is.EqualTo("1234"));
        }
        finally
        {
            store.Remove(key);
        }

        Assert.That(store.Get(key), Is.Null);
    }

    [Test]
    public async Task GetSliceAsync()
    {
        const string key = "slice";
        using var store = CreateStore();

        try
        {
            await store.AddAsync(key, "0123456789");
            var buffer = await store.GetSliceAsync(key, 1, 4);
            Assert.That(buffer, Is.Not.Null);
            Assert.That(Encoding.UTF8.GetString(buffer), Is.EqualTo("1234"));
        }
        finally
        {
            await store.RemoveAsync(key);
        }

        Assert.That(await store.GetAsync(key), Is.Null);
    }

    [Test]
    public void CanListEntriesWithinPrefix()
    {
        const string key1 = "first";
        const string key2 = "second";
        const string otherKey = "outside-prefix";
        using var store = CreateStore();
        using var otherStore = CreateStoreFor($"{nameof(CanListEntriesWithinPrefix)}-other");

        try
        {
            store.Add(key1, new byte[3]);
            store.Add(key2, new byte[5]);
            otherStore.Add(otherKey, new byte[7]);

            AssertEntries(store.List(), (key1, 3L), (key2, 5L));
        }
        finally
        {
            store.Remove(key1);
            store.Remove(key2);
            otherStore.Remove(otherKey);
        }
    }

    [Test]
    public async Task CanListEntriesWithinPrefixAsync()
    {
        const string key1 = "first";
        const string key2 = "second";
        using var store = CreateStore();

        try
        {
            await store.AddAsync(key1, new byte[3]);
            await store.AddAsync(key2, new byte[5]);

            var entries = new List<(string key, long size)>();
            await foreach (var entry in store.ListAsync())
            {
                entries.Add(entry);
            }

            AssertEntries(entries, (key1, 3L), (key2, 5L));
        }
        finally
        {
            await store.RemoveAsync(key1);
            await store.RemoveAsync(key2);
        }
    }
}
