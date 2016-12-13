package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"realtime-processing/kafka"
	"realtime-processing/model"
)

const (
	topicOrderLog    = "order_logs"
	topicPageviewLog = "page_view_logs"
	topicClickLog    = "click_logs"
)

func main() {
	partitionTopicOrderConsumer, partitionOrderOffsetManager, err := kafka.Consume(topicOrderLog, 0)
	if err != nil {
		panic(err)
	}

	partitionTopicPageviewConsumer, partitionPageviewOffsetManager, err := kafka.Consume(topicPageviewLog, 0)
	if err != nil {
		panic(err)
	}

	partitionTopicClickConsumer, partitionTopicOffsetManager, err := kafka.Consume(topicClickLog, 0)
	if err != nil {
		panic(err)
	}

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	consumed := 0

	log.Println("Listening from Kafka...")

ConsumerLoop:
	for {
		select {
		case msg := <-partitionTopicOrderConsumer.Messages():
			log.Printf("Consumed message offset %d\n", msg.Offset)
			consumed++

			var event model.Event
			err := json.Unmarshal(msg.Value, &event)
			if err != nil {
				fmt.Println("Invalid json: ", err)
			} else {
				fmt.Printf("%+v\n", event)
			}

			partitionOrderOffsetManager.MarkOffset(msg.Offset+1, "metadata")
		case msg := <-partitionTopicPageviewConsumer.Messages():
			log.Printf("Consumed message offset %d\n", msg.Offset)
			consumed++

			var event model.Event
			err := json.Unmarshal(msg.Value, &event)
			if err != nil {
				fmt.Println("Invalid json: ", err)
			} else {
				fmt.Printf("%+v\n", event)
			}

			partitionPageviewOffsetManager.MarkOffset(msg.Offset+1, "metadata")
		case msg := <-partitionTopicClickConsumer.Messages():
			log.Printf("Consumed message offset %d\n", msg.Offset)
			consumed++

			var event model.Event
			err := json.Unmarshal(msg.Value, &event)
			if err != nil {
				fmt.Println("Invalid json: ", err)
			} else {
				fmt.Printf("%+v\n", event)
			}

			partitionTopicOffsetManager.MarkOffset(msg.Offset+1, "metadata")
		case <-signals:
			break ConsumerLoop
		}
	}

	log.Printf("Consumed: %d\n", consumed)
}
