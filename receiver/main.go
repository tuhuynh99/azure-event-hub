package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/checkpoints"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

func handleError(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {

	// TODO: replace <storage-account-name> with your actual storage account name
	url := "https://forgoapps.blob.core.windows.net/event-hub-reveiver"
	// ctx := context.Background()

	credential, err := azidentity.NewDefaultAzureCredential(nil)
	handleError(err)

	// client, err := container.NewClient(url, credential, nil)
	handleError(err)
	fmt.Print("authensuccessfully")
	// // create a container client using a connection string and container name
	client, err := container.NewClient(url, credential, nil)

	if err != nil {
		panic(err)
	}

	// create a checkpoint store that will be used by the event hub
	checkpointStore, err := checkpoints.NewBlobStore(client, nil)

	if err != nil {
		panic(err)
	}

	// create a consumer client using a connection string to the namespace and the event hub
	consumerClient, err := azeventhubs.NewConsumerClientFromConnectionString("<eventhubnamespace_connectionstring>", "eventhubname", azeventhubs.DefaultConsumerGroup, nil)

	if err != nil {
		panic(err)
	}

	defer consumerClient.Close(context.TODO())

	// create a processor to receive and process events
	processor, err := azeventhubs.NewProcessor(consumerClient, checkpointStore, nil)

	if err != nil {
		panic(err)
	}

	//  for each partition in the event hub, create a partition client with processEvents as the function to process events
	dispatchPartitionClients := func() {
		for {
			partitionClient := processor.NextPartitionClient(context.TODO())

			if partitionClient == nil {
				break
			}

			go func() {
				if err := processEvents(partitionClient); err != nil {
					panic(err)
				}
			}()
		}
	}

	// run all partition clients
	go dispatchPartitionClients()

	processorCtx, processorCancel := context.WithCancel(context.TODO())
	defer processorCancel()

	if err := processor.Run(processorCtx); err != nil {
		panic(err)
	}
}

func processEvents(partitionClient *azeventhubs.ProcessorPartitionClient) error {
	defer closePartitionResources(partitionClient)
	for {
		receiveCtx, receiveCtxCancel := context.WithTimeout(context.TODO(), time.Minute)
		events, err := partitionClient.ReceiveEvents(receiveCtx, 100, nil)
		receiveCtxCancel()

		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		fmt.Printf("Processing %d event(s)\n", len(events))

		for _, event := range events {
			fmt.Printf("Event received with body %v\n", string(event.Body))
		}

		if len(events) != 0 {
			if err := partitionClient.UpdateCheckpoint(context.TODO(), events[len(events)-1], nil); err != nil {
				return err
			}
		}
	}
}

func closePartitionResources(partitionClient *azeventhubs.ProcessorPartitionClient) {
	defer partitionClient.Close(context.TODO())
}
