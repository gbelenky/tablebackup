using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices(s =>
    {
        string connectionString = Environment.GetEnvironmentVariable("BlobStorageConnection");
        s.AddSingleton(x => new BlobContainerClient (new Uri(connectionString)));
    })
    .Build();

host.Run();
