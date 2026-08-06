package library

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// getkafkawriter builds a producer (writer) bound to a topic on the given brokers.

func GetKafkaWriter(brokers []string, topic string) *kafka.Writer {

	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},    // picks partition by hashing the key
		RequiredAcks: kafka.RequireAll, // wait for all in-sync replicas before ack
	}
}

// publishMessage marshals payload to JSON and writes it with an optional key.
// Mirrors publishtoNats
func PublishMessage(ctx context.Context, w *kafka.Writer, key string, payload interface{}) error {

	value, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Marshalling failed: %v", err)

		return err

	}

	if err := w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
		Time:  time.Now(),
	}); err != nil {
		log.Printf("Failed to publish to kafka: %v", err)
		return err
	}

	return nil
}
