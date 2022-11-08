using NUnit.Framework;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Uncodium.SimpleStore.Tests;

[TestFixture]
public class SimpleAzureBlobStoreTests
{
    private static readonly string PREFIX = $"tests/{DateTimeOffset.UtcNow:O}";

    private const string SAS =
        "https://tmpfast.blob.core.windows.net/simpleazureblobstoretests" +
        "?sv=2021-08-06&st=2022-11-08T17%3A47%3A54Z&se=2022-11-08T18%3A47%3A00Z" +
        "&sr=c&sp=racwdxltf&sig=ejjdObTv%2BdarvqmUcH0e1o1OdZixzwXH1q0goRTHyu0%3D"
        ;

    [Test]
    public void CanCreateStoreFromSas()
    {
        using var store = new SimpleAzureBlobStore(SAS);
    }

    [Test]
    public void CanCreateStoreFromSasAndPrefix()
    {
        using var store = new SimpleAzureBlobStore(SAS, PREFIX);
    }

    [Test]
    public void CanAddGetRemoveExists()
    {
        var key = nameof(CanAddGetRemoveExists);
        using var store = new SimpleAzureBlobStore(SAS, PREFIX);

        store.Add(key, "my content");

        var x = Encoding.UTF8.GetString(store.Get(key));
        Assert.IsTrue(x == "my content");

        store.Remove(key);

        var exists = store.Get(key);
        Assert.IsTrue(exists == null);
    }

    [Test]
    public async Task CanAddGetRemoveExistsAsync()
    {
        var key = nameof(CanAddGetRemoveExistsAsync);
        using var store = new SimpleAzureBlobStore(SAS, PREFIX);

        await store.AddAsync(key, "my content");

        var x = Encoding.UTF8.GetString(await store.GetAsync(key));
        Assert.IsTrue(x == "my content");

        await store.RemoveAsync(key);

        var exists = await store.GetAsync(key);
        Assert.IsTrue(exists == null);
    }

    [Test]
    public void GetWriteStream()
    {
        var key = nameof(GetWriteStream);
        using var store = new SimpleAzureBlobStore(SAS, PREFIX);

        using (var stream = store.GetWriteStream(key))
        {
            new MemoryStream(Encoding.UTF8.GetBytes("my stream content")).CopyTo(stream);
        }

        var x = Encoding.UTF8.GetString(store.Get(key));
        Assert.IsTrue(x == "my stream content");

        store.Remove(key);

        var exists = store.Get(key);
        Assert.IsTrue(exists == null);
    }

    [Test]
    public async Task GetWriteStreamAsync()
    {
        var key = nameof(GetWriteStreamAsync);
        using var store = new SimpleAzureBlobStore(SAS, PREFIX);

        using (var stream = await store.GetWriteStreamAsync(key))
        {
            await new MemoryStream(Encoding.UTF8.GetBytes("my stream content")).CopyToAsync(stream);
        }

        var x = Encoding.UTF8.GetString(await store.GetAsync(key));
        Assert.IsTrue(x == "my stream content");

        await store.RemoveAsync(key);

        var exists = await store.GetAsync(key);
        Assert.IsTrue(exists == null);
    }

    [Test]
    public void GetReadStream()
    {
        var key = nameof(GetReadStream);
        using var store = new SimpleAzureBlobStore(SAS, PREFIX);

        using (var stream = store.GetWriteStream(key))
        {
            new MemoryStream(Encoding.UTF8.GetBytes("my stream content")).CopyTo(stream);
        }

        using (var readStream = store.GetStream(key))
        {
            var ms = new MemoryStream();
            readStream.CopyTo(ms);
            var s = Encoding.UTF8.GetString(ms.ToArray());
            Assert.IsTrue(s == "my stream content");
        }

        store.Remove(key);

        var exists = store.Get(key);
        Assert.IsTrue(exists == null);
    }

    [Test]
    public async Task GetReadStreamAsync()
    {
        var key = nameof(GetReadStreamAsync);
        using var store = new SimpleAzureBlobStore(SAS, PREFIX);

        using (var stream = await store.GetWriteStreamAsync(key))
        {
            await new MemoryStream(Encoding.UTF8.GetBytes("my stream content")).CopyToAsync(stream);
        }

        using (var readStream = await store.GetStreamAsync(key))
        {
            var ms = new MemoryStream();
            await readStream.CopyToAsync(ms);
            var s = Encoding.UTF8.GetString(ms.ToArray());
            Assert.IsTrue(s == "my stream content");
        }

        await store.RemoveAsync(key);

        var exists = await store.GetAsync(key);
        Assert.IsTrue(exists == null);
    }
}
