package main

import (
	"github.com/exsql-io/go-datastore/services"
	"os"
	"sync"
)

func main() {
	configuration, err := LoadConfiguration(os.Getenv("EXSQL_DATASTORE_SERVER_CONFIGURATION_PATH"))
	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}

	tailer, err := services.NewTailer(configuration.InstanceId, configuration.Brokers, configuration.Streams[0].Topic, &wg)
	if err != nil {
		panic(err)
	}

	defer tailer.Stop()
	tailer.Start()

	leaf, err := services.NewLeaf(configuration.Streams[0].Schema, tailer.Channel, &wg)
	if err != nil {
		panic(err)
	}

	defer leaf.Stop()
	leaf.Start()

	wg.Wait()
}
