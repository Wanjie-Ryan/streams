package library

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// # connection -> channel -> queue -> publish -> consume
// Dial a connection -> open a channel -> declare a queue -> publish

// GetRabbitConnection opens a connection. The library owns client creation

func GetRabbitConnection(url string) *amqp.Connection {

	conn, err := amqp.Dial(url)

	if err != nil {
		log.Fatalf("failed to connect to Rabbit: %v", err)
	}

	return conn

}

// declarequeue ensures a queue exists, and returns it
// the queue holds messages and delivers to consumers
// every queue get binded to the default exchange ""
func DeclareQueue(ch *amqp.Channel, name string) amqp.Queue {

	q, err := ch.QueueDeclare(
		name,  // queuename
		false, // durable -> survive broker restart
		false, // autoDelete,
		false, //exclusive
		false, //noWait
		nil,   // args
	)

	if err != nil {
		log.Fatalf("failed to declare queue %s: %v", name, err)
	}

	return q

}

// publishmessage sends JSON to the default exchange, routed by queue name

func PublishMessage(ctx context.Context, ch *amqp.Channel, queueName string, payload interface{}) error {

	body, err := json.Marshal(payload)
	if err != nil {

		fmt.Printf("failed to marashal payload: %v", err)

		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return ch.PublishWithContext(
		ctx,
		"",        // exchange: "" = default exchange
		queueName, // routing key: default exhange delivers to the queue with this exact name
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)

}
