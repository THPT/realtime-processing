package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"realtime-processing/kafka"
	"realtime-processing/model"
	"realtime-processing/redis"
	"strconv"
	"time"
)

const (
	topicOrderLog    = "order_logs"
	topicPageviewLog = "page_view_logs"
	topicClickLog    = "click_logs"

	userHLL = "userHLL"
)

func main() {
	//init redis
	redis.InitRedis()
	defer redis.CloseRedis()

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
				addHLLVisitor(event.Uuid)
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
				addHLLVisitor(event.Uuid)
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
				addHLLVisitor(event.Uuid)
			}

			partitionTopicOffsetManager.MarkOffset(msg.Offset+1, "metadata")
		case <-signals:
			break ConsumerLoop
		}
	}

	log.Printf("Consumed: %d\n", consumed)
}

func addHLLVisitor(uuid string) {
	min := time.Now().Minute()
	key := userHLL + "_" + strconv.Itoa(min)
	res := redis.Redis.PFAdd(key, uuid)
	if res != nil && res.Err() != nil {
		fmt.Println(res.Err())
	}
	if re := redis.Redis.Expire(key, 10*time.Minute); re != nil {
		if re.Err() != nil {
			fmt.Println(res.Err())
		}
	}
}
