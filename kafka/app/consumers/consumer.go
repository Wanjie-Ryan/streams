package consumers

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)


type Consumer struct{
	Reader *kafka.Reader
}

func (c *Consumer) Start(ctx context.Context){

	cfg := c.Reader.Config()
	log.Printf("consumer started | topic=%s group=%s", cfg.Topic, cfg.GroupID)

	for{
		msg, err := c.Reader.ReadMessage(ctx) // fetches and Auto-commits the offset

		if err !=nil{
			log.Printf("consumer stopped: %v", err) // ctx cancelled

			return
		}

		log.Printf("consumed | partition=%d offset=%d key=%s value=%s", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
	}

	// the function also autocommits the bookmark. After readMessage returns a record, kafka records that this group has consumed up to that offset. So the next time you resume past it

}