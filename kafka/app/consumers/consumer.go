package consumers

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	Reader *kafka.Reader
	DLQ    *kafka.Writer
}

// func (c *Consumer) Start(ctx context.Context) {

// 	cfg := c.Reader.Config()
// 	log.Printf("consumer started | topic=%s group=%s", cfg.Topic, cfg.GroupID)

// 	for {
// 		// the readmessage auto-commits and marks the message done the instant it hands it to you, before your code processes it.
// 		msg, err := c.Reader.ReadMessage(ctx) // fetches and Auto-commits the offset

// 		if err != nil {
// 			log.Printf("consumer stopped: %v", err) // ctx cancelled

// 			return
// 		}

// 		log.Printf("consumed | partition=%d offset=%d key=%s value=%s", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

// 		// commit ONLY after success
// 		if err := c.Reader.CommitMessages(ctx, msg); err != nil {
// 			log.Printf("commit failed: %v", err)
// 		}

// 	}

// 	// the function also autocommits the bookmark. After readMessage returns a record, kafka records that this group has consumed up to that offset. So the next time you resume past it

// }

func (c *Consumer) Start(ctx context.Context) {

	topic := c.Reader.Config().Topic
	log.Printf("consumer started | topic=%s group=%s", topic, c.Reader.Config().GroupID)

	for {
		msg, err := c.Reader.FetchMessage(ctx) // fetch, don't commit yet
		if err != nil {
			log.Printf("consumer stopped: %v", err)
			return
		}

		if err := c.process(msg); err != nil {
			log.Printf("giving up after retries | offset=%d err=%v -> DLQ", msg.Offset, err)
			if derr := c.toDLQ(ctx, topic, msg, err); derr != nil {
				log.Printf("DLQ publish failed, will reprocess on restart:%v", derr)
				continue // skip commit -> message is retried on next restart
			}
		}

		if err := c.Reader.CommitMessages(ctx, msg); err != nil { // advance bookmark
			log.Printf("commit failed: %v", err)
		}
	}

}

// process - bounded in-place retries with backoff  (avoids freezing the partition forever)

func (c *Consumer) process(msg kafka.Message) error {

	const maxAttempts = 3

	var err error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err = handle(msg); err == nil {
			return nil
		}

		log.Printf("attempt %d/%d failed | offset=%d err=%v", attempt, maxAttempts, msg.Offset, err)
		time.Sleep(time.Duration(attempt) * 200 * time.Millisecond) // backoff
	}

	return err
}

// handle is the real work, here it deliberately fails for key posion so you can see the DLQ in action
func handle(msg kafka.Message) error {
	if string(msg.Key) == "poison" {
		return fmt.Errorf("poison message cannot be processed")
	}

	log.Printf("processed ok | partition=%d offset=%d key=%s", msg.Partition, msg.Offset, string(msg.Key))
	return nil
}

// toDLQ publishes the failed message to <topic>.DLT withdiagnostic headers.
func (c *Consumer) toDLQ(ctx context.Context, originalTopic string, msg kafka.Message, cause error) error{
	return c.DLQ.WriteMessages(ctx, kafka.Message{
		Key: msg.Key,
		Value: msg.Value,
		Headers: []kafka.Header{
			{Key: "x-error", Value: []byte(cause.Error())},
			{Key: "x-original-topic", Value: []byte(originalTopic)},
			{Key: "x-original-partition", Value: []byte(fmt.Sprintf("%d", msg.Partition))},
			{Key: "x-original-offset", Value: []byte(fmt.Sprintf("%d", msg.Offset))},
		},
	})
}
