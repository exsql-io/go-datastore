package main

import (
	"context"
	"github.com/apache/arrow/go/v13/arrow/compute"
	"github.com/apache/arrow/go/v13/arrow/compute/exprs"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/exsql-io/go-datastore/services"
	"github.com/exsql-io/go-datastore/store"
	"github.com/labstack/echo/v4"
	"github.com/substrait-io/substrait-go/expr"
	"log"
	"net/http"
	"os"
)

func main() {
	configuration, err := LoadConfiguration(os.Getenv("EXSQL_DATASTORE_SERVER_CONFIGURATION_PATH"))
	if err != nil {
		panic(err)
	}

	tailer, err := services.NewTailer(configuration.InstanceId, configuration.Brokers, configuration.Streams[0].Topic)
	if err != nil {
		panic(err)
	}

	defer tailer.Stop()
	tailer.Start()

	schema, inputFormatType := configuration.Streams[0].Schema, configuration.Streams[0].Format
	leaf, err := services.NewLeaf(schema, inputFormatType, tailer.Channel)
	if err != nil {
		panic(err)
	}

	defer leaf.Stop()
	leaf.Start()

	e := echo.New()
	e.GET("/streams/:topic", func(context echo.Context) error { return getStream(leaf, context) })
	e.GET("/streams/:topic/query/:name", func(context echo.Context) error {
		name := context.Param("name")
		err := query(leaf, name, context)
		if err != nil {
			log.Println(err.Error())
		}

		return err
	})

	e.GET("/streams/:topic/:key", func(context echo.Context) error { return getStreamValueByKey(leaf, context) })

	e.Logger.Fatal(e.Start(":1323"))
}

func getStream(leaf *services.Leaf, context echo.Context) error {
	iterator, err := (*leaf.Store).Iterator()
	if err != nil {
		return err
	}

	defer (*iterator).Close()

	return iteratorResponse(iterator, context)
}

func getStreamValueByKey(leaf *services.Leaf, context echo.Context) error {
	value := (*leaf.Store).Get([]byte(context.Param("key")))
	return context.JSONBlob(http.StatusOK, value)
}

func query(leaf *services.Leaf, name string, echoContext echo.Context) error {
	schema := (*leaf.Store).Schema()
	extensionSet := exprs.NewDefaultExtensionSet()
	builder := exprs.NewExprBuilder(extensionSet)
	err := builder.SetInputSchema(schema)
	if err != nil {
		return err
	}

	call, err := builder.CallScalar(
		"equal",
		nil,
		builder.FieldRef("name"),
		builder.Literal(expr.NewPrimitiveLiteral(name, false)))

	if err != nil {
		return err
	}

	ex, err := call.BuildExpr()
	if err != nil {
		return err
	}

	iterator, err := (*leaf.Store).Iterator(func(input compute.Datum) (compute.Datum, error) {
		ctx := exprs.WithExtensionIDSet(compute.WithAllocator(context.Background(), memory.DefaultAllocator), extensionSet)
		return exprs.ExecuteScalarExpression(ctx, schema, ex, input)
	})

	if err != nil {
		return err
	}

	defer (*iterator).Close()

	return iteratorResponse(iterator, echoContext)
}

func iteratorResponse(iterator *store.CloseableIterator, context echo.Context) error {
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
