package consumer

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	Channel *amqp.Channel
	Queue   string
}

func (c *Consumer) Start(ctx context.Context) {

	deliveries, err := c.Channel.Consume(
		c.Queue, // queue
		"",      // consumer tag
		true,    //auto-ack
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatalf("failed to register consumer: %v", err)
	}

	log.Printf("consumer started || queue=%s", c.Queue)

	for {
		select {
		case <-ctx.Done():
			log.Printf("consumer stopped:%v", ctx.Err())
			return
		case d, ok := <-deliveries:
			if !ok {
				log.Printf("deliveries channel closed")
				return
			}

			log.Printf("consumed || body=%s", string(d.Body))
		}
	}

}
