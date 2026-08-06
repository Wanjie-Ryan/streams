package router

import (
	"log"
	"os"
	"strings"

	"github.com/segmentio/kafka-go"

	"github.com/Wanjie-Ryan/streams/kafka/app/library"
)

type App struct {
	KafkaWriter *kafka.Writer
}

func (a *App) Initialize() {
	brokers := getBrokers()
	topic := os.Getenv("KAFKA_TOPIC")
	a.KafkaWriter = library.GetKafkaWriter(brokers, topic)

	log.Printf("kafka writer is read || brokers=%v topic%s", brokers, topic)

}

func getBrokers() []string {

	brokerConnection := os.Getenv("KAFKA_BROKERS")

	return strings.Split(brokerConnection, ",")
}
