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
using Azure;


namespace GBelenky.TableBackup
{
    public class TableBackup
    {
        private readonly BlobContainerClient _blobContainerClient;
        private readonly TableClient _tableClient;
        private string backupBlob = "backup.json";
        public TableBackup(BlobContainerClient blobContainerClient, TableClient tableClient)
        {
            _blobContainerClient = blobContainerClient;
            _tableClient = tableClient;
        }
    
        [Function("TableBackupSingleActivity")]
        public async Task RunOrchestratorSingleActivity(
                     [OrchestrationTrigger] TaskOrchestrationContext context)
        {
            ILogger logger = context.CreateReplaySafeLogger("TableBackupSingleActivity");
            string backupBlobSetting = Environment.GetEnvironmentVariable("BackupBlob") ?? "backup.json";

            DateTime now = context.CurrentUtcDateTime;
            string filePrefix = context.InstanceId + "-SingleActivity-";
            backupBlob = filePrefix + backupBlobSetting;

            logger.LogInformation($"Starting backup at: {now}}}");

            int rowCount = await context.CallActivityAsync<int>(nameof(BackupAllSingleActivity), backupBlob);

            DateTime end = context.CurrentUtcDateTime;
            logger.LogInformation($"Backup completed with Total Rows: {rowCount} Total Time seconds : {(end - now).TotalSeconds}"); ;
            return;
        }


        [Function(nameof(BackupAllSingleActivity))]
        public async Task<int> BackupAllSingleActivity([ActivityTrigger] string backupBlob, FunctionContext executionContext)
        {
            string? continuationToken = null;
            bool moreResultsAvailable = true;
            int pageSizeSetting = Int32.Parse(Environment.GetEnvironmentVariable("PageSize") ?? "1000");
            
            int rows = 0;
            while (moreResultsAvailable)
            {
                Azure.Page<TableEntity>? page = _tableClient
                    .Query<TableEntity>()
                    .AsPages(continuationToken, pageSizeHint: pageSizeSetting)
                    .FirstOrDefault(); // Note: Since the pageSizeHint only limits the number of results in a single page, we explicitly only enumerate the first page.

                if (page == null)
                    break;

                // Get the continuation token from the page.
                // Note: This value can be stored so that the next page query can be executed later.
                continuationToken = page.ContinuationToken;

                IReadOnlyList<TableEntity> pageResults = page.Values;
                moreResultsAvailable = pageResults.Any() && continuationToken != null;
                rows += pageResults.Count;
                TableEntity[] valuesArray = pageResults.ToArray();
                
                var appendBlobClient = _blobContainerClient.GetAppendBlobClient(backupBlob);
                // Create the append blob if it doesn't exist
                await appendBlobClient.CreateIfNotExistsAsync();
                var json = JsonSerializer.Serialize(valuesArray);
                byte[] bytes = Encoding.UTF8.GetBytes(json);
                // Write the bytes to the append blob
                using (var memoryStream = new MemoryStream(bytes))
                {
                    await appendBlobClient.AppendBlockAsync(memoryStream);
                }
         
            }
            return rows;
        }

       
        [Function("TableBackup_HttpStart_SingleActivity")]
        public static async Task<HttpResponseData> HttpStartSingleActivity(
          [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequestData req,
          [DurableClient] DurableTaskClient client,
          FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("TableBackup_HttpStart_SingleActivity");

            // Function input comes from the request content.
            DateTime now = DateTime.UtcNow;
            StartOrchestrationOptions? options = new StartOrchestrationOptions()
            {
                InstanceId = $"{now.Year}-{now.Month}-{now.Day}-{now.Hour}-{now.Minute}-{now.Second}"
            };
            string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
                "TableBackupSingleActivity", options: options, input: null);

            logger.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

            // Returns an HTTP 202 response with an instance management payload.
            // See https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-http-api#start-orchestration
            return client.CreateCheckStatusResponse(req, instanceId);
        }
    }
}
