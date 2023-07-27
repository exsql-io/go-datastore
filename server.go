package main

import (
	"github.com/exsql-io/go-datastore/common"
	"github.com/exsql-io/go-datastore/engine"
	"github.com/exsql-io/go-datastore/services"
	"github.com/goccy/go-json"
	"github.com/labstack/echo/v4"
	"io"
	"log"
	"net/http"
	"os"
)

func main() {
	configuration, err := LoadConfiguration(os.Getenv("EXSQL_DATASTORE_SERVER_CONFIGURATION_PATH"))
	if err != nil {
		panic(err)
	}

	var tailers = make(map[string]*services.Tailer)
	var leafs = make(map[string]*services.Leaf)

	for _, stream := range configuration.Streams {
		tailer, err := services.NewTailer(configuration.InstanceId, configuration.Brokers, stream.Topic)
		if err != nil {
			panic(err)
		}

		tailer.Start()

		tailers[stream.Topic] = tailer

		schema, inputFormatType := stream.Schema, stream.Format
		leaf, err := services.NewLeaf(stream.Topic, schema, inputFormatType, tailer.Channel)
		if err != nil {
			panic(err)
		}

		leaf.Start()

		leafs[stream.Topic] = leaf
	}

	volcanoEngine := engine.VolcanoEngine{}

	e := echo.New()
	e.GET("/streams/:name", func(context echo.Context) error { return getStream(leafs, context) })
	e.POST("/streams/sql", func(echoContext echo.Context) error {
		body, err := io.ReadAll(echoContext.Request().Body)
		if err != nil {
			log.Println(err.Error())
			return err
		}

		var message map[string]string
		err = json.Unmarshal(body, &message)
		if err != nil {
			log.Println(err.Error())
			return err
		}

		p, err := engine.Plan(leafs, message["query"])
		if err != nil {
			return err
		}

		iterator, err := volcanoEngine.Execute(leafs, p)
		if err != nil {
			return err
		}

		return iteratorResponse(iterator, echoContext)
	})

	e.Logger.Fatal(e.Start(":1323"))
}

func getStream(leafs map[string]*services.Leaf, context echo.Context) error {
	name := context.Param("name")
	leaf := leafs[name]

	iterator, err := (*leaf.Store).Iterator()
	if err != nil {
		return err
	}

	defer (*iterator).Close()

	return iteratorResponse(iterator, context)
}

func iteratorResponse(iterator *common.CloseableIterator, context echo.Context) error {
	defer (*iterator).Close()

	context.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	context.Response().WriteHeader(http.StatusOK)

	writer := context.Response()
	for (*iterator).Next() {
		batch := (*iterator).Value()

		count := (*batch).NumRows()
		for i := int64(0); i < count; i++ {
			record := (*batch).NewSlice(i, i+1)
			bytes, err := record.MarshalJSON()
			if err != nil {
				return err
			}

			_, err = writer.Write(bytes[1 : len(bytes)-1])
			if err != nil {
				return err
			}

			writer.Flush()
			record.Release()
		}

		(*batch).Release()
	}

	return nil
}
