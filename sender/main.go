package main

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
)

func main() {
	// create an Event Hubs producer client using a connection string to the namespace and the event hub
	producerClient, err := azeventhubs.NewProducerClientFromConnectionString("Endpoint=sb://leiaevh.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=XNydvAM+OC75u+FoIUxnXCFJcVzWKHLcF+AEhGPy+F8=", "hub1", nil)

	if err != nil {
		panic(err)
	}

	defer producerClient.Close(context.TODO())

	// create sample events
	events := createEventsForSample()

	// create a batch object and add sample events to the batch
	newBatchOptions := &azeventhubs.EventDataBatchOptions{}

	batch, err := producerClient.NewEventDataBatch(context.TODO(), newBatchOptions)

	if err != nil {
		panic(err)
	}

	for i := 0; i < len(events); i++ {
		err = batch.AddEventData(events[i], nil)

		if err != nil {
			panic(err)
		}
	}

	// send the batch of events to the event hub
	err = producerClient.SendEventDataBatch(context.TODO(), batch, nil)

	if err != nil {
		panic(err)
	}
}

func createEventsForSample() []*azeventhubs.EventData {
	return []*azeventhubs.EventData{
		{
			Body: []byte("hello"),
		},
		{
			Body: []byte("world"),
		},
	}
}
