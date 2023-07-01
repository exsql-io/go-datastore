package main

import (
	"fmt"
	"github.com/exsql-io/go-datastore/services"
	"github.com/exsql-io/go-datastore/store"
	"github.com/go-co-op/gocron"
	"os"
	"sync"
	"time"
)

func main() {
	scheduler := gocron.NewScheduler(time.Local)

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

	schema, inputFormatType := configuration.Streams[0].Schema, configuration.Streams[0].Format
	leaf, err := services.NewLeaf(schema, inputFormatType, tailer.Channel, &wg)
	if err != nil {
		panic(err)
	}

	defer leaf.Stop()
	leaf.Start()

	_, err = scheduler.Every(1).Seconds().Do(func(store *store.InMemoryStore) {
		iterator, err := store.Iterator()
		if err != nil {
			panic(err)
		}

		fmt.Println("Ingested records from:", tailer.Topic)
		defer (*iterator).Close()

		for (*iterator).Next() {
			record := (*iterator).Value()
			json, err := (*record).MarshalJSON()
			if err != nil {
				panic(err)
			}

			fmt.Printf("record: %s\n", string(json))
		}
	}, leaf.Store)

	if err != nil {
		panic(err)
	}

	scheduler.StartAsync()
	wg.Wait()
}
