package router

import (
	"log"
	"os"
	"strings"

	"github.com/segmentio/kafka-go"

	"github.com/Wanjie-Ryan/streams/kafka/app/library"
)

type App struct {
	Brokers     []string
	Topic       string
	KafkaWriter *kafka.Writer
}

func (a *App) Initialize() {
	a.Brokers = getBrokers()
	a.Topic = os.Getenv("KAFKA_TOPIC")
	a.KafkaWriter = library.GetKafkaWriter(a.Brokers, a.Topic)

	log.Printf("kafka writer is read || brokers=%v topic=%s", a.Brokers, a.Topic)

}

func getBrokers() []string {

	brokerConnection := os.Getenv("KAFKA_BROKERS")

	return strings.Split(brokerConnection, ",")
}
