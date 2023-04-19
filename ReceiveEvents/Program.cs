using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;

const string connectionString = "Endpoint=sb://hackergamepass0401.servicebus.windows.net/;SharedAccessKeyName=sender;SharedAccessKey=R+P2ozg5E52qn2zypTBoEn658le0LM/qk+AEhCeY5e0=;EntityPath=test-event-hub";
const string eventHubName = "test-event-hub";
const string blobStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=hackergamepass;AccountKey=Xii/PW4qITaYyDgOyzdWSIETKKSKTziRCZiuw6rwIsJAimvzCUZdCwdgqEVi0j6FlrDV0imlSg1K+AStvvJCIw==;EndpointSuffix=core.windows.net";
const string blobContainerName = "test-blob";
string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

// Create a blob container client that the event processor will use 
BlobContainerClient storageClient = new BlobContainerClient(blobStorageConnectionString, blobContainerName);

// Create an event processor client to process events in the event hub
EventProcessorClient processor = new EventProcessorClient(storageClient, consumerGroup, connectionString, eventHubName);

// Register handlers for processing events and handling errors
processor.ProcessEventAsync += ProcessEventHandler;
processor.ProcessErrorAsync += ProcessErrorHandler;

// Start the processing
await processor.StartProcessingAsync();

// Wait for 10 seconds for the events to be processed
await Task.Delay(TimeSpan.FromSeconds(1000));

// Stop the processing
async Task ProcessEventHandler(ProcessEventArgs eventArgs)
{
    // Write the body of the event to the console window
    Console.WriteLine("\tRecevied event: {0}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));

    // Update checkpoint in the blob storage so that the app receives only new events the next time it's run
    await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
}

Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
{
    // Write details about the error to the console window
    Console.WriteLine($"\tPartition '{eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
    Console.WriteLine(eventArgs.Exception.Message);
    return Task.CompletedTask;
}
