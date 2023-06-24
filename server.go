package main

import (
	"errors"
	"fmt"
	"github.com/exsql-io/go-datastore/services"
	"log"
)

func main() {
	tailer, err := services.NewTailer("tailer", []string{"localhost:9092"}, "quickstart-events")
	if err != nil {
		panic(err)
	}

	defer tailer.Stop()

	tailer.Start()
	for tailer.IsRunning {
		message := <-tailer.Channel
		if len(message.Errors) > 0 {
			for _, err := range message.Errors {
				log.Fatalln(err)
			}

			panic(errors.New("an error occurred while consuming topic"))
		}

		fmt.Println("topic:", message.Record.Topic, "record value:", string(message.Record.Value))
	}
}
