using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using System.Text.Json;
using Azure;
using System.Text;
using Azure.Storage.Blobs.Specialized;


namespace GBelenky.TableBackup
{
    public class TableBackup
    {
        private readonly BlobContainerClient _blobContainerClient;

        public TableBackup(BlobContainerClient blobContainerClient)
        {
            _blobContainerClient = blobContainerClient;
        }


        [Function(nameof(TableBackup))]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] TaskOrchestrationContext context)
        {
            ILogger logger = context.CreateReplaySafeLogger(nameof(TableBackup));

            logger.LogInformation("Reading all table entities");

            var tableConnectionString = Environment.GetEnvironmentVariable("TableStorageConnection");
            var tableClient = new TableClient(new Uri(tableConnectionString));

            int pageSize = 1000;
            string continuationToken = null;
            do
            {
                var page = tableClient.Query<TableEntity>().AsPages(continuationToken, pageSize).First();
                // convert page.Values to json
                var json = JsonSerializer.Serialize(page.Values);
                await context.CallActivityAsync(nameof(BackupPage), json);
                continuationToken = page.ContinuationToken;
            } while (continuationToken != null);


            var outputs = new List<string>();

            return outputs;
        }

       [Function(nameof(BackupPage))]
        public async Task BackupPage([ActivityTrigger] string pageValues, FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("BackupPage");
            logger.LogInformation("Writing page to blob storage");
            var backupBlob = Environment.GetEnvironmentVariable("BackupBlob");
            // Get a reference to the container and the append blob
            var appendBlobClient = _blobContainerClient.GetAppendBlobClient(backupBlob);

            // Create the append blob if it doesn't exist
            await appendBlobClient.CreateIfNotExistsAsync();

            // Convert the JSON string to bytes
            byte[] bytes = Encoding.UTF8.GetBytes(pageValues);

            // Write the bytes to the append blob
            using (var memoryStream = new MemoryStream(bytes))
            {
                await appendBlobClient.AppendBlockAsync(memoryStream);
            }

            return;
        }

/*
        [Function(nameof(BackupPage))]
        public async Task BackupPage([ActivityTrigger] string pageValues, FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("BackupPage");
            logger.LogInformation("Writing page to blob storage");
            var backupBlobContainer = Environment.GetEnvironmentVariable("BackupBlobContainer");
            var backupBlob = Environment.GetEnvironmentVariable("BackupBlob");
            // Get a reference to the container and the append blob
            var containerClient = _blobClient.GetBlobContainerClient(backupBlobContainer);
            var appendBlobClient = containerClient.GetAppendBlobClient(backupBlob);

            // Create the append blob if it doesn't exist
            await appendBlobClient.CreateIfNotExistsAsync();

            // Convert the JSON string to bytes
            byte[] bytes = Encoding.UTF8.GetBytes(pageValues);

            // Write the bytes to the append blob
            using (var memoryStream = new MemoryStream(bytes))
            {
                await appendBlobClient.AppendBlockAsync(memoryStream);
            }

            return;
        }
*/

        // read all tables uris from storage account        
        [Function(nameof(ReadTable))]
        public static async Task ReadTable([ActivityTrigger] FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("ReadTable");
            logger.LogInformation("Reading all table entities");

            var tableConnectionString = Environment.GetEnvironmentVariable("TableStorageConnection");
            var tableClient = new TableClient(new Uri(tableConnectionString));

            int pageSize = 1000;
            string continuationToken = null;
            do
            {
                var page = tableClient.Query<TableEntity>().AsPages(continuationToken, pageSize).First();
                int i = 0;
                foreach (TableEntity qEntity in page.Values)
                {
                    logger.LogInformation($"Number in page:{i} RowKey: {qEntity.GetString("RowKey")}");
                    i++;
                }
                // Call your method here after processing each page
                logger.LogInformation($"******************Entities returned: {page.Values.Count()}******************");
                continuationToken = page.ContinuationToken;
            } while (continuationToken != null);
            /*           
                        Pageable<TableEntity> queryResultsFilter = tableClient.Query<TableEntity>();

                        foreach (TableEntity qEntity in queryResultsFilter)
                        {
                            logger.LogInformation($"RowKey: {qEntity.GetString("RowKey")}");
                        }

                         logger.LogInformation($"The query returned {queryResultsFilter.Count()} entities.");
            */
            // save entities to blob storage
            /*
            var blobConnectionString = Environment.GetEnvironmentVariable("BlobStorageConnection");
            var blobClient = new BlobClient(new Uri(blobConnectionString));
            var json = JsonSerializer.Serialize(entities);
            var bytes = Encoding.UTF8.GetBytes(json);
            var stream = new MemoryStream(bytes);
            await blobClient.UploadAsync(stream);
            */
            return;
        }

        [Function("TableBackup_HttpStart")]
        public static async Task<HttpResponseData> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequestData req,
            [DurableClient] DurableTaskClient client,
            FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("TableBackup_HttpStart");

            // Function input comes from the request content.
            string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
                nameof(TableBackup));

            logger.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

            // Returns an HTTP 202 response with an instance management payload.
            // See https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-http-api#start-orchestration
            return client.CreateCheckStatusResponse(req, instanceId);
        }
    }
}
