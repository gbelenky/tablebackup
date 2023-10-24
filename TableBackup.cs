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
        private readonly TableClient _tableClient;
        public TableBackup(BlobContainerClient blobContainerClient, TableClient tableClient)
        {
            _blobContainerClient = blobContainerClient;
            _tableClient = tableClient;
        }


        [Function(nameof(TableBackup))]
        public static async Task RunOrchestrator(
             [OrchestrationTrigger] TaskOrchestrationContext context)
        {
            ILogger logger = context.CreateReplaySafeLogger(nameof(TableBackup));
            var backupBlobSetting = Environment.GetEnvironmentVariable("BackupBlob");

            DateTime now = context.CurrentUtcDateTime;
            //string filePrefix = $"{now.Year}-{now.Month}-{now.Day}-{now.Hour}-{now.Minute}-{now.Second}-";
            string filePrefix = context.InstanceId + "-";
            string backupBlob = filePrefix + backupBlobSetting;

            int pageSizeSetting = Int32.Parse(Environment.GetEnvironmentVariable("PageSize"));
            // min valid date for Azure Table Storage
            DateTime earliestRowDate = new DateTime(1601, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            logger.LogInformation($"Starting backup at: {now}}}");

            TimeStampParams timeStampParams = new TimeStampParams { NextTimeSlot = earliestRowDate, PageSize = pageSizeSetting, BackupBlob = backupBlob };
            DateTime? lastRowDate = earliestRowDate;
            int pages = 0;
            while ( DateTime.Compare((DateTime)lastRowDate, DateTime.MinValue) != 0)
            {
                lastRowDate = await context.CallActivityAsync<DateTime?>(nameof(BackupPageByTimestamp), timeStampParams);
                if (lastRowDate != null)
                {
                    timeStampParams.NextTimeSlot = (DateTime)lastRowDate;
                }
                pages++;
                logger.LogInformation($"Page {pages} with page size {pageSizeSetting} backed up");
            }

            DateTime end = context.CurrentUtcDateTime;
            logger.LogInformation($"Backup completed at: {end}");
            return;
        }

        [Function(nameof(BackupPageByTimestamp))]
        public async Task<DateTime> BackupPageByTimestamp([ActivityTrigger] TimeStampParams aParams, FunctionContext executionContext)
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
            await foreach (Azure.Page<TableEntity> page in entities.AsPages())
            {
                // get last timestamp
                foreach (TableEntity entity in page.Values)
                {
                    lastRowDate = entity.GetDateTime("Timestamp");
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
            return (DateTime)lastRowDate;
        }

        [Function("TableBackup_HttpStart")]
        public static async Task<HttpResponseData> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequestData req,
            [DurableClient] DurableTaskClient client,
            FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("TableBackup_HttpStart");

            // Function input comes from the request content.
            DateTime now = DateTime.UtcNow;            
            StartOrchestrationOptions? options = new StartOrchestrationOptions()
            {
                InstanceId = $"{now.Year}-{now.Month}-{now.Day}-{now.Hour}-{now.Minute}-{now.Second}"
            };  
            string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
                nameof(TableBackup), options: options, input: null);

            logger.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

            // Returns an HTTP 202 response with an instance management payload.
            // See https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-http-api#start-orchestration
            return client.CreateCheckStatusResponse(req, instanceId);
        }
    }

    public class TimeStampParams
    {
        public DateTime NextTimeSlot { get; set; }
        public int PageSize { get; set; }
        public string BackupBlob { get; set; }
    }
}
