using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using System.Text.Json;
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
        public static async Task RunOrchestrator(
             [OrchestrationTrigger] TaskOrchestrationContext context)
        {
            ILogger logger = context.CreateReplaySafeLogger(nameof(TableBackup));
            logger.LogInformation("Reading all table entities");

            var tableConnectionString = Environment.GetEnvironmentVariable("TableStorageConnection");
            var tableClient = new TableClient(new Uri(tableConnectionString));
            var backupBlob = Environment.GetEnvironmentVariable("BackupBlob");
            
            DateTime now = context.CurrentUtcDateTime;
            string filePrefix = $"{now.Year}-{now.Month}-{now.Day}-{now.Hour}-{now.Minute}-{now.Second}-";
            backupBlob = filePrefix + backupBlob;

            int pageSize = 1000;
            string? continuationToken = null;
            do
            {
                var page = tableClient.Query<TableEntity>().AsPages(continuationToken, pageSize).First();
                var json = JsonSerializer.Serialize(page.Values);
                ActivityParams activityParams = new ActivityParams { Json = json, BackupBlob = backupBlob };
                await context.CallActivityAsync(nameof(BackupPage), activityParams);
                continuationToken = page.ContinuationToken;
            } while (continuationToken != null);

            return;
        }

        [Function(nameof(BackupPage))]
        public async Task BackupPage([ActivityTrigger] ActivityParams aParams, FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("BackupPage");
            logger.LogInformation("Writing page to blob storage");
            

            // Get a reference to the container and the append blob
            var appendBlobClient = _blobContainerClient.GetAppendBlobClient(aParams.BackupBlob);

            // Create the append blob if it doesn't exist
            await appendBlobClient.CreateIfNotExistsAsync();

            // Convert the JSON string to bytes
            byte[] bytes = Encoding.UTF8.GetBytes(aParams.Json);

            // Write the bytes to the append blob
            using (var memoryStream = new MemoryStream(bytes))
            {
                await appendBlobClient.AppendBlockAsync(memoryStream);
            }

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

    public class ActivityParams
    {
        public string Json { get; set; }
        public string BackupBlob { get; set; }
    }
}
