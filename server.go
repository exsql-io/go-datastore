package main

import (
	"github.com/exsql-io/go-datastore/services"
	"sync"
)

func main() {
	wg := sync.WaitGroup{}

	tailer, err := services.NewTailer("tailer", []string{"localhost:9092"}, "quickstart-events", &wg)
	if err != nil {
		panic(err)
	}

	defer tailer.Stop()
	tailer.Start()

	var schema services.Schema
	leaf, err := services.NewLeaf(schema, tailer.Channel, &wg)
	if err != nil {
		panic(err)
	}

	defer leaf.Stop()
	leaf.Start()

	wg.Wait()
}
