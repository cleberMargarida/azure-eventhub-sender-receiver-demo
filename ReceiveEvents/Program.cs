using Azure.Core;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using Ingestion.Api.Dto;
using Newtonsoft.Json;
using Serilog;
using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

BlobContainerClient storageClient = new BlobContainerClient(
    connectionString: "DefaultEndpointsProtocol=https;AccountName=checkpointstoremb;AccountKey=0rDx40FQNpfikhnvQOOvQ7yeHVkH0hC5BIYvD/OPlkJreqwb1yN8ekGfMRjx6L0oRAe6wUREb/BD+ASt4567Jg==;EndpointSuffix=core.windows.net",
    blobContainerName: "signallogcheckpoint"
);

TokenCredential credential = new ClientSecretCredential(
    tenantId: "9652d7c2-1ccf-4940-8151-4a92bd474ed0",
    clientId: "23ef8095-16a5-46f9-b8eb-3b46c45450aa",
    clientSecret: "8Wk8Q~BrjTUDYSEr1jHfCDElMYgEhKKuicvtBaoJ"
);

EventProcessorClient processor = new EventProcessorClient(
    checkpointStore: storageClient,
    consumerGroup: "cdbstream",
    fullyQualifiedNamespace: "csg-weu-prod-smp-eh-mbbstream.servicebus.windows.net",
    eventHubName: "csg-weu-prod-smp-eh-streaming-mbbstream",
    credential: credential
);

Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.File("log.txt", rollingInterval: RollingInterval.Day)
            .CreateLogger();

Log.Information("HashCode,DeviceId,SignalName,Value,TimeStamp,DateTimeUTC,PartitionId,SequenceNumber,EnqueuedTime");

AppDomain.CurrentDomain.ProcessExit += FlushLog;

const int EventsBeforeCheckpoint = 50;
ConcurrentDictionary<string, int> PartitionEventCount = new ConcurrentDictionary<string, int>();
ConcurrentDictionary<Guid, int> DuplicatedSignalsCount = new ConcurrentDictionary<Guid, int>();

processor.ProcessEventAsync += ProcessEventHandler;
processor.ProcessErrorAsync += ProcessErrorHandler;

await processor.StartProcessingAsync();
await Task.Delay(Timeout.Infinite);

async Task ProcessEventHandler(ProcessEventArgs eventArgs)
{
    LogAsync(eventArgs);

    int eventsSinceLastCheckpoint = PartitionEventCount.AddOrUpdate(
        key: eventArgs.Partition.PartitionId,
        addValue: 1,
        updateValueFactory: (_, currentCount) => currentCount + 1);

    if (eventsSinceLastCheckpoint >= EventsBeforeCheckpoint)
    {
        await eventArgs.UpdateCheckpointAsync();
        PartitionEventCount[eventArgs.Partition.PartitionId] = 0;
    }
}

Task LogAsync(ProcessEventArgs eventArgs)
{
    var @event = JsonConvert.SerializeObject(eventArgs.Data.Properties);
    var message = Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray());

    var signals = JsonConvert.DeserializeObject<Signals>(message, new JsonSerializerSettings()
    {
        Culture = CultureInfo.InvariantCulture,
    });

    foreach (var signal in signals.Items)
    {
        var copiesCount = DuplicatedSignalsCount.AddOrUpdate(
        key: signal.HashCode,
        addValue: 1,
        updateValueFactory: HandleDuplicate);

        if (copiesCount > 1)
        {
            continue;
        }

        Log.Information(string.Join(",", new[]
        {
            signal.HashCode,
            eventArgs.Data.Properties["deviceId"],
            signal.SignalName,
            signal.Value?.ToString() ?? JsonConvert.SerializeObject(signal.KeyValue),
            signal.TimeStamp.ToString(),
            signal.DateTimeUTC.ToString(),
            eventArgs.Partition.PartitionId,
            eventArgs.Data.SequenceNumber.ToString(),
            eventArgs.Data.EnqueuedTime.ToString(),
        }));
    }

    return Task.CompletedTask;
}

int HandleDuplicate(Guid key, int currentCount)
{
    Console.WriteLine($"Find duplicated to signal: {key}");
    return ++currentCount;
}

Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
{
    Console.WriteLine(eventArgs.Exception);
    return Task.CompletedTask;
}

void FlushLog(object sender, EventArgs e)
{
    Log.CloseAndFlush();
}
