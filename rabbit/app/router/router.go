package router

import (
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Wanjie-Ryan/streams/rabbit/app/library"
)

type App struct {
	Conn    *amqp.Connection
	Channel *amqp.Channel
	Queue   string
}

func (a *App) Initialize() {

	rabbitUrl := os.Getenv("RABBITMQ_URL")

	a.Conn = library.GetRabbitConnection(rabbitUrl)

	ch, err := a.Conn.Channel() // one TCP connection

	if err != nil {
		log.Fatalf("failed to open channel: %v", err)

	}

	a.Channel = ch

	a.Queue = os.Getenv("RABBITMQ_QUEUE")

	library.DeclareQueue(a.Channel, a.Queue)

	log.Printf("rabbitmq ready || queue=%s", a.Queue)

}
