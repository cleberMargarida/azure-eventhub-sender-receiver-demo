using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

const string connectionString = "Endpoint=sb://hackergamepass0401.servicebus.windows.net/;SharedAccessKeyName=sender;SharedAccessKey=R+P2ozg5E52qn2zypTBoEn658le0LM/qk+AEhCeY5e0=;EntityPath=test-event-hub";
const string eventHubName = "test-event-hub";

// Create a producer client that you can use to send events to an event hub
await using var producerClient = new EventHubProducerClient(connectionString, eventHubName);

foreach (var i in Enumerable.Range(0, 100))
{
    // Create a batch of events 
    using var eventBatch = await producerClient.CreateBatchAsync();
    // Add events to the batch. An event is a represented by a collection of bytes and metadata. 
    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(i.ToString())));
    // Use the producer client to send the batch of events to the event hub
    await producerClient.SendAsync(eventBatch);
    Console.WriteLine($"Publish: {i}");
    await Task.Delay(1000);
}
