package kafka

import (
	"log"
	"realtime-processing/config"

	"github.com/Shopify/sarama"
)

const (
	kafkaGroup = "order_group"
)

func Consume(topic string, partitionID int32) (sarama.PartitionConsumer, sarama.PartitionOffsetManager, error) {
	client, err := sarama.NewClient([]string{config.Config.Host + ":" + config.Config.Port}, sarama.NewConfig())
	if err != nil {
		return nil, nil, err
	}

	offsetManager, err := sarama.NewOffsetManagerFromClient(kafkaGroup, client)
	if err != nil {
		return nil, nil, err
	}

	partitionOffsetManager, err := offsetManager.ManagePartition(topic, partitionID)
	if err != nil {
		return nil, nil, err
	}

	consumer, err := sarama.NewConsumer([]string{config.Config.Host + ":" + config.Config.Port}, nil)
	if err != nil {
		return nil, nil, err
	}
	nx, _ := partitionOffsetManager.NextOffset()
	partitionOrderConsumer, err := consumer.ConsumePartition(topic, partitionID, nx)
	if err != nil {
		return nil, nil, err
	}

	return partitionOrderConsumer, partitionOffsetManager, nil
}

func CloseConnection(partitionOrderConsumer sarama.PartitionConsumer, partitionOffsetManager sarama.PartitionOffsetManager) {
	if err := partitionOffsetManager.Close(); err != nil {
		log.Println(err)
	}
	if err := partitionOrderConsumer.Close(); err != nil {
		log.Println(err)
	}
}
