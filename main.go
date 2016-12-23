package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"realtime-processing/infra"
	"realtime-processing/model"
	"strconv"
	"time"
)

const (
	topicOrderLog    = "order_logs"
	topicPageviewLog = "page_view_logs"
	topicClickLog    = "click_logs"

	userHLL           = "userHLL"
	videoViewCountKey = "video_view"
	locationCountKey  = "location"
)

func main() {
	//init redis
	infra.InitRedis()
	defer infra.CloseRedis()

	//Consume topic order
	partitionTopicOrderConsumer, partitionOrderOffsetManager, err := infra.Consume(topicOrderLog, 0)
	if err != nil {
		panic(err)
	}

	//Consume topic pageview
	partitionTopicPageviewConsumer, partitionPageviewOffsetManager, err := infra.Consume(topicPageviewLog, 0)
	if err != nil {
		panic(err)
	}

	//Consume topic click
	partitionTopicClickConsumer, partitionTopicOffsetManager, err := infra.Consume(topicClickLog, 0)
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
				increaseVideoViewCount(event.VideoId)
				increaseLocationCount(event.Location)
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
	res := infra.Redis.PFAdd(key, uuid)
	if res != nil && res.Err() != nil {
		fmt.Println(res.Err())
		return
	}
	if re := infra.Redis.Expire(key, 10*time.Minute); re != nil {
		if re.Err() != nil {
			fmt.Println(re.Err())
		}
	}
}

func increaseVideoViewCount(videoId string) {
	now := time.Now()
	min := now.Minute()
	hour := now.Hour()
	timer := min + hour*60
	key := videoViewCountKey + "_" + strconv.Itoa(timer)

	if res := infra.Redis.HIncrBy(key, videoId, 1); res != nil {
		if err := res.Err(); err != nil {
			fmt.Println(err)
			return
		}
	}

	if re := infra.Redis.Expire(key, 60*2*time.Minute); re != nil {
		if re.Err() != nil {
			fmt.Println(re.Err())
		}
	}
}

func increaseLocationCount(location string) {
	now := time.Now()
	min := now.Minute()
	hour := now.Hour()
	timer := min + hour*60
	for i := 0; i < 6; i++ {
		t := timer - i
		if t < 0 {
			t += 60 * 24
		}
		key := locationCountKey + "_" + strconv.Itoa(t)
		if res := infra.Redis.HIncrBy(key, location, 1); res != nil {
			if err := res.Err(); err != nil {
				fmt.Println(err)
				return
			}
		}

		if re := infra.Redis.Expire(key, 60*2*time.Minute); re != nil {
			if re.Err() != nil {
				fmt.Println(re.Err())
			}
		}
	}
}
