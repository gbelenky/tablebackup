using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Azure.Storage.Blobs;
using Azure.Data.Tables;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices(s =>
    {
        string? blobConnectionString = Environment.GetEnvironmentVariable("BlobStorageConnection");
        string? tableStorageConnection = Environment.GetEnvironmentVariable("TableStorageConnection");
        if (tableStorageConnection != null)
        {
            s.AddSingleton(x => new TableClient(new Uri(tableStorageConnection)));
        }
        if (blobConnectionString != null)
        {
            s.AddSingleton(x => new BlobContainerClient(new Uri(blobConnectionString)));
        }
    })
    .Build();

host.Run();
