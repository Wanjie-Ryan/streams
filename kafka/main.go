package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"github.com/Wanjie-Ryan/streams/kafka/app/consumers"
	"github.com/Wanjie-Ryan/streams/kafka/app/library"
	"github.com/Wanjie-Ryan/streams/kafka/app/router"
)

func main() {

	err := godotenv.Load()

	if err != nil {
		log.Fatalf("Failed to load env")

	}

	app := &router.App{}
	app.Initialize()
	// defer app.KafkaWriter.Close()

	if os.Getenv("MODE") == "consumer" {
		runConsumer(app)
		return
	}

	runProducer(app)

	// for i :=1; i<=5; i++ {
	// 	payload := map[string] any{"id": i, "message": "intro to kafka"}
	// 	if err := library.PublishMessage(context.Background(), app.KafkaWriter, "user-1", payload); err !=nil{
	// 		log.Fatalf("Failed to publish message: %v", err)
	// 	}
	// 	log.Printf("published message %d", i)
	// }
	// log.Printf("done publishing")

}

func runProducer(app *router.App) {
	defer app.KafkaWriter.Close()

	for i := 1; i <= 5; i++ {
		payload := map[string]any{"id": i, "message": "intro to kafka"}
		if err := library.PublishMessage(context.Background(), app.KafkaWriter, "user-1", payload); err != nil {
			log.Fatalf("Failed to publish message: %v", err)
		}
		log.Printf("published message %d", i)
	}
	log.Printf("done publishing")
}

func runConsumer(app *router.App) {

	reader := library.GetKafkaReader(app.Brokers, app.Topic, os.Getenv("KAFKA_GROUP_ID"))

	defer reader.Close() // leave the group cleanly & commits final offsets

	// graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	(&consumers.Consumer{Reader: reader}).Start(ctx)

}
