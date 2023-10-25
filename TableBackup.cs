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

 [Function("TableBackupByTimestamp")]
        public async Task RunOrchestratorByTimestamp(
             [OrchestrationTrigger] TaskOrchestrationContext context)
        {
            ILogger logger = context.CreateReplaySafeLogger("TableBackupByTimestamp");
            string backupBlobSetting = Environment.GetEnvironmentVariable("BackupBlob") ?? "backup.json";
            int pageSizeSetting = Int32.Parse(Environment.GetEnvironmentVariable("PageSize") ?? "1000");

            DateTime now = context.CurrentUtcDateTime;
            string filePrefix = context.InstanceId + "-ByTimestamp-";
            backupBlob = filePrefix + backupBlobSetting;
            
            pageSizeSetting = Int32.Parse(Environment.GetEnvironmentVariable("PageSize") ?? "1000");

            // min valid date for Azure Table Storage
            DateTime earliestRowDate = new DateTime(1601, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            logger.LogInformation($"Starting backup at: {now}}}");

            TimeStampParams timeStampParams = new TimeStampParams { NextTimeSlot = earliestRowDate, PageSize = pageSizeSetting, BackupBlob = backupBlob };
            DateTime lastRowDate = earliestRowDate;
            int pages = 0;
            int rowCount = 0;

            while (DateTime.Compare(lastRowDate, DateTime.MinValue) != 0)
            {
                BackupResponse bResponse = await context.CallActivityAsync<BackupResponse>(nameof(BackupPageByTimestamp), timeStampParams);
                if (bResponse != null)
                {
                    lastRowDate = bResponse.lastRowDate;
                    timeStampParams.NextTimeSlot = lastRowDate;
                    pages++;
                    rowCount += bResponse.rowCount;
                    logger.LogInformation($"Backup of Total Rows: {rowCount} Pages: {pages} Last row date: {lastRowDate}");
                }
            }
            DateTime end = context.CurrentUtcDateTime;
            logger.LogInformation($"Backup completed with Total Rows: {rowCount} Pages: {pages} Last row date: {lastRowDate} Total Time seconds : {(end - now).TotalSeconds}");;
            return;
        }

        [Function("TableBackupPageByQuery")]
        public async Task RunOrchestratorPageByQuery(
             [OrchestrationTrigger] TaskOrchestrationContext context)
        {
            ILogger logger = context.CreateReplaySafeLogger("TableBackupPageByQuery");
            string backupBlobSetting = Environment.GetEnvironmentVariable("BackupBlob") ?? "backup.json";
            int pageSizeSetting = Int32.Parse(Environment.GetEnvironmentVariable("PageSize") ?? "1000");

            DateTime now = context.CurrentUtcDateTime;
            string filePrefix = context.InstanceId + "-PageByQuery-";
            backupBlob = filePrefix + backupBlobSetting;

            string continuationToken = null;
            bool moreResultsAvailable = true;
            int backupBytes = 0;
            int rows = 0;
            while (moreResultsAvailable)
            {
                Azure.Page<TableEntity> page = _tableClient
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
                BackupPageByQueryParams backupPageByQueryParams = new BackupPageByQueryParams { BackupBlob = backupBlob, Values = valuesArray };
                backupBytes += await context.CallActivityAsync<int>(nameof(BackupPageByQuery), backupPageByQueryParams);
            }
            DateTime end = context.CurrentUtcDateTime;
            logger.LogInformation($"Backup completed with Total Rows: {rows} Total Bytes: {backupBytes} Total Time seconds : {(end - now).TotalSeconds}");
        }

        [Function(nameof(BackupPageByQuery))]
        public async Task<int> BackupPageByQuery([ActivityTrigger] BackupPageByQueryParams backupPageByQueryParams, FunctionContext executionContext)
        {
            var appendBlobClient = _blobContainerClient.GetAppendBlobClient(backupPageByQueryParams.BackupBlob);
            // Create the append blob if it doesn't exist
            await appendBlobClient.CreateIfNotExistsAsync();
            var json = JsonSerializer.Serialize(backupPageByQueryParams.Values);
            byte[] bytes = Encoding.UTF8.GetBytes(json);
            // Write the bytes to the append blob
            using (var memoryStream = new MemoryStream(bytes))
            {
                await appendBlobClient.AppendBlockAsync(memoryStream);
            }
            return bytes.Length;
        }

        [Function(nameof(BackupPageByTimestamp))]
        public async Task<BackupResponse> BackupPageByTimestamp([ActivityTrigger] TimeStampParams aParams, FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("BackupPageByTimestamp");
            string nextTimeSlotString = aParams.NextTimeSlot.ToString("o");
            // Get a reference to the container and the append blob
            var appendBlobClient = _blobContainerClient.GetAppendBlobClient(aParams.BackupBlob);

            // Create the append blob if it doesn't exist
            await appendBlobClient.CreateIfNotExistsAsync();

            var queryFilter = TableClient.CreateQueryFilter($"Timestamp gt datetime{nextTimeSlotString}");
            logger.LogInformation($"query filter: {queryFilter} with page size: {aParams.PageSize}");

            Azure.AsyncPageable<TableEntity> entities = _tableClient.QueryAsync<TableEntity>(
                filter: queryFilter, maxPerPage: aParams.PageSize);

            // Get the first page of results
            DateTime? lastRowDate = new DateTime();
            int rowCount = 0;
            await foreach (Azure.Page<TableEntity> page in entities.AsPages())
            {
                // get last timestamp
                foreach (TableEntity entity in page.Values)
                {
                    lastRowDate = entity.GetDateTime("Timestamp");
                    rowCount++;
                    logger.LogInformation($"BackupId: {aParams.BackupBlob} PartitionKey: {entity.PartitionKey} EntityKey: {entity.RowKey}");
                }

                var json = JsonSerializer.Serialize(page.Values);
                byte[] bytes = Encoding.UTF8.GetBytes(json);
                // Write the bytes to the append blob
                using (var memoryStream = new MemoryStream(bytes))
                {
                    await appendBlobClient.AppendBlockAsync(memoryStream);
                }
                break; // Exit after the first page
            }
            return new BackupResponse { rowCount = rowCount, lastRowDate = (DateTime)lastRowDate };
        }

        [Function("TableBackup_HttpStart_ByTimestamp")]
        public static async Task<HttpResponseData> HttpStartByTimestamp(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequestData req,
            [DurableClient] DurableTaskClient client,
            FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("TableBackup_HttpStart_ByTimestamp");

            // Function input comes from the request content.
            DateTime now = DateTime.UtcNow;
            StartOrchestrationOptions? options = new StartOrchestrationOptions()
            {
                InstanceId = $"{now.Year}-{now.Month}-{now.Day}-{now.Hour}-{now.Minute}-{now.Second}"
            };
            string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
                "TableBackupByTimestamp", options: options, input: null);

            logger.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

            // Returns an HTTP 202 response with an instance management payload.
            // See https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-http-api#start-orchestration
            return client.CreateCheckStatusResponse(req, instanceId);
        }

        [Function("TableBackup_HttpStart_PageByQuery")]
        public static async Task<HttpResponseData> HttpStartPageByQuery(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequestData req,
            [DurableClient] DurableTaskClient client,
            FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("TableBackup_HttpStart_PageByQuery");

            // Function input comes from the request content.
            DateTime now = DateTime.UtcNow;
            StartOrchestrationOptions? options = new StartOrchestrationOptions()
            {
                InstanceId = $"{now.Year}-{now.Month}-{now.Day}-{now.Hour}-{now.Minute}-{now.Second}"
            };
            string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
                "TableBackupPageByQuery", options: options, input: null);

            logger.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

            // Returns an HTTP 202 response with an instance management payload.
            // See https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-http-api#start-orchestration
            return client.CreateCheckStatusResponse(req, instanceId);
        }
    }

    public class TimeStampParams
    {
        public DateTime NextTimeSlot { get; set; } = DateTime.MinValue;
        public int PageSize { get; set; }
        public required string BackupBlob { get; set; }
    }

    public class BackupResponse
    {
        public int rowCount { get; set; } = 0;
        public DateTime lastRowDate { get; set; } = DateTime.MinValue;
    }

    public class BackupPageByQueryParams
    {
        public required string BackupBlob { get; set; }
        public required TableEntity[] Values { get; set; }
    }

}
