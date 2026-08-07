package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"github.com/Wanjie-Ryan/streams/rabbit/app/consumer"
	"github.com/Wanjie-Ryan/streams/rabbit/app/library"
	"github.com/Wanjie-Ryan/streams/rabbit/app/router"
)

func main() {
	err := godotenv.Load()

	if err != nil {
		log.Fatalf("Failed to load env: %v", err)

	}

	app := &router.App{}
	app.Initialize()
	defer app.Conn.Close()
	defer app.Channel.Close()

	if os.Getenv("MODE") == "consumer" {
		runConsumer(app)
		return
	}

	runProducer(app)

}

func runProducer(app *router.App) {
	for i := 1; i <= 5; i++ {
		payload := map[string]any{"id": i, "message": "hello rabbitmq"}
		if err := library.PublishMessage(context.Background(), app.Channel, app.Queue, payload); err != nil {
			log.Fatalf("publish failed: %v", err)
		}
		log.Printf("published message %d", i)
	}
	log.Printf("done publishing")
}

func runConsumer(app *router.App) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	(&consumer.Consumer{Channel: app.Channel, Queue: app.Queue}).Start(ctx)
}
