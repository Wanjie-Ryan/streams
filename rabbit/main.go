package main

import (
	"log"

	"github.com/joho/godotenv"

	"github.com/Wanjie-Ryan/streams/rabbit/app/router"
)

func main() {
	err := godotenv.Load()

	if err != nil {
		log.Fatalf("Failed to load env: %v", err)

	}

	app := &router.App{}
	app.Initialize()

}
