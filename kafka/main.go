package main

import (
	"context"
	"log"

	"github.com/joho/godotenv"

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
	defer app.KafkaWriter.Close()

	for i :=1; i<=5; i++ {
		payload := map[string] any{"id": i, "message": "intro to kafka"}
		if err := library.PublishMessage(context.Background(), app.KafkaWriter, "user-1", payload); err !=nil{
			log.Fatalf("Failed to publish message: %v", err)
		}
		log.Printf("published message %d", i)
	}
	log.Printf("done publishing")

}
