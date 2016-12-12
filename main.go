package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"realtime-processing/config"
	"realtime-processing/model"
	"time"

	"github.com/Shopify/sarama"
)

const (
	kafkaGroup      = "order_group"
	kafkaConsumerID = "realtime_processing"
)

func main() {
	broker := sarama.NewBroker(config.Config.Host + ":" + config.Config.Port)
	err := broker.Open(nil)
	if err != nil {
		panic(err)
	}

	// request := sarama.MetadataRequest{Topics: []string{"order_logs"}}
	// response, err := broker.GetMetadata(&request)
	// if err != nil {
	// 	_ = broker.Close()
	// 	panic(err)
	// }
	// fmt.Printf("%+v\n", response)

	reqOffset := sarama.OffsetRequest{}
	res, err := broker.GetAvailableOffsets(&reqOffset)
	if err != nil {
		panic(err)
	}

	resBlockOffset := res.GetBlock("order_logs", 0)
	var offset int64
	if resBlockOffset != nil {
		offset = resBlockOffset.Offset
	}
	fmt.Println("resBlockOffset", resBlockOffset)

	offsetFecthReq := sarama.OffsetFetchRequest{
		ConsumerGroup: kafkaGroup,
	}
	ofRes, err := broker.FetchOffset(&offsetFecthReq)
	if err != nil {
		panic(err)
	}

	resBlockOffseta := ofRes.GetBlock("order_logs", 0)
	if resBlockOffseta != nil {
		offset = resBlockOffseta.Offset
	}
	fmt.Println("resBlockOffseta", resBlockOffseta)

	consumer, err := sarama.NewConsumer([]string{config.Config.Host + ":" + config.Config.Port}, nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition("order_logs", 0, offset)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0

ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Consumed message offset %d\n", msg.Offset)
			consumed++

			var event model.Event
			err := json.Unmarshal(msg.Value, &event)
			if err != nil {
				fmt.Println("Invalid json: ", err)
			} else {
				fmt.Printf("%+v\n", event)
			}

			offsetCommitRequest := sarama.OffsetCommitRequest{
				ConsumerGroup: kafkaGroup,
				ConsumerID:    kafkaConsumerID,
			}
			offsetCommitRequest.AddBlock("order_logs", 0, msg.Offset, time.Now().Unix(), "metadata")
			res, err := broker.CommitOffset(&offsetCommitRequest)
			fmt.Println(res, err)
		case <-signals:
			break ConsumerLoop
		}
	}

	log.Printf("Consumed: %d\n", consumed)
}
