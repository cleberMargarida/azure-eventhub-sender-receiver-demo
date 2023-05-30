using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

ConcurrentDictionary<string, Timer> _partitionEventTimer = new ConcurrentDictionary<string, Timer>();

BlobContainerClient storageClient = new BlobContainerClient(
    connectionString: "",
    blobContainerName: ""
);

var ConnectionString = "";
EventProcessorClient processor = new EventProcessorClient(storageClient, "$Default", ConnectionString);

processor.ProcessEventAsync += ProcessEventHandler;
processor.ProcessErrorAsync += ProcessErrorHandler;
processor.PartitionInitializingAsync += ProcessInitHandler;
processor.PartitionClosingAsync += ProcessCloseHandler;

await processor.StartProcessingAsync();
await Task.Delay(Timeout.Infinite);

async Task ProcessEventHandler(ProcessEventArgs eventArgs)
{
    Console.WriteLine("Receive msg from partition " + eventArgs.Partition.PartitionId);
    Console.WriteLine(Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));

    if (!_partitionEventTimer.ContainsKey(eventArgs.Partition.PartitionId))
    {
        _partitionEventTimer[eventArgs.Partition.PartitionId] = StartTimer(eventArgs);
    }

    UpdateCheckpointAsync(eventArgs);
}

void UpdateCheckpointAsync(object obj)
{
    Console.WriteLine("Updating the checkpoint.");
    var arg = (ProcessEventArgs)obj;
    arg.UpdateCheckpointAsync().Wait();
    _partitionEventTimer[arg.Partition.PartitionId].Dispose();
    _partitionEventTimer[arg.Partition.PartitionId] = StartTimer(arg);
}

Timer StartTimer(ProcessEventArgs eventArg)
{
    return new Timer(UpdateCheckpointAsync, eventArg, TimeSpan.FromSeconds(10), Timeout.InfiniteTimeSpan);
}

Task ProcessInitHandler(PartitionInitializingEventArgs arg)
{
    Console.WriteLine("partition init " + arg.PartitionId);
    return Task.CompletedTask;
}

Task ProcessCloseHandler(PartitionClosingEventArgs arg)
{
    Console.WriteLine("partition close " + arg.PartitionId);
    return Task.CompletedTask;
}

Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
{
    Console.WriteLine(eventArgs.Exception);
    return Task.CompletedTask;
}

//Log.Logger = new LoggerConfiguration()
//            .MinimumLevel.Debug()
//            .WriteTo.File("log.txt", rollingInterval: RollingInterval.Day)
//            .CreateLogger();
//AppDomain.CurrentDomain.ProcessExit += FlushLog;


//Task LogAsync(ProcessEventArgs eventArgs)
//{
//    var @event = JsonConvert.SerializeObject(eventArgs.Data.Properties);
//    var message = Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray());

//    Log.Information(new { DataProperties = @event, Message = message }.ToString());

//    return Task.CompletedTask;
//}


//int HandleDuplicate(Guid key, int currentCount)
//{
//    Console.WriteLine($"Find duplicated to signal: {key}");
//    return ++currentCount;
//}

//void FlushLog(object sender, EventArgs e)
//{
//    Log.CloseAndFlush();
//}
