package main

import (
	"github.com/exsql-io/go-datastore/services"
)

func main() {
	tailer, err := services.NewTailer("tailer", []string{"localhost:9092"}, "quickstart-events")
	if err != nil {
		panic(err)
	}

	defer tailer.Stop()
	tailer.Start()

	var schema services.Schema
	leaf, err := services.NewLeaf(schema, tailer.Channel)
	if err != nil {
		panic(err)
	}

	leaf.Start()
	defer leaf.Stop()
}
