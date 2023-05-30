using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System;
using System.Text;

const string connectionString = "";
await using var producerClient = new EventHubProducerClient(connectionString);

const string MessageBase =
"""
{
    "signals":
    [
        {
            "id": 6054,
            "timestamp": 1682289369443,
            "treatment": 1,
            "value": 100.755
        }
    ]
}
""";

while (true)
{
    using var eventBatch = await producerClient.CreateBatchAsync();
    var message = MessageBase.Replace("1682289369443", UtcNowTimeStamp());
    var eventData = new EventData(Encoding.UTF8.GetBytes(message.ToString()));
    eventData.Properties["deviceId"] = "ctp-3620003708";

    eventData.Properties["bsId"] = "d34ddc59-dc25-4132-ad20-dd9b6707ddc0";
    eventData.Properties["ctpDispatchedTimestamp"] = 1683729553872;
    eventData.Properties["processedTimestamp"] = 1683729701585;
    eventData.Properties["vin"] = "9BM384078PB267453";
    eventData.Properties["ctpSendCounter"] = 7616;
    eventData.Properties["iothubEnqueuedTimestamp"] = "2023-05-10T14:41:40.193Z";
    eventData.Properties["version"] = 3;
    eventData.Properties["trackingId"] = "ctp-0460009013-1683729699591-17391";

    eventBatch.TryAdd(eventData);   
    Console.WriteLine("Press any key to enqueue...");
    Console.ReadKey();
    await producerClient.SendAsync(eventBatch);
    Console.WriteLine("message enqueued: " + message);
}

static string UtcNowTimeStamp() => ((long)(DateTime.UtcNow - DateTime.UnixEpoch).TotalMilliseconds).ToString();

/*
"""
    "signals":
    [
         {"id":"5555","timestamp":1683124730000,"value":null,"keyValue":[{"Key":500,"Value":0.0},{"Key":501,"Value":24.200003},{"Key":504,"Value":-22.889584},{"Key":505,"Value":-42.03309},{"Key":506,"Value":12.0},{"Key":507,"Value":0.0},{"Key":503,"Value":225.642},{"Key":502,"Value":1.76}]}
    ]

    "signals":
    [
        {
            "id": 6054,
            "timestamp": 1682289369443,
            "treatment": 1,
            "value": 12.755
        }
    ]
"""; 

{
    "bsId": "d34ddc59-dc25-4132-ad20-dd9b6707ddc0",
    "ctpDispatchedTimestamp": 1683729553872,
    "processedTimestamp": 1683729701585,
    "vin": "9BM384078PB267453",
    "ctpSendCounter": 7616,
    "deviceId": "ctp-0460009013",
    "iothubEnqueuedTimestamp": "2023-05-10T14:41:40.193Z",
    "version": 3,
    "trackingId": "ctp-0460009013-1683729699591-17391"
} 

 */