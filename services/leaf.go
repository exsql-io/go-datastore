package services

import (
	"context"
	"errors"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/exsql-io/go-datastore/common"
	"github.com/exsql-io/go-datastore/store"
	"log"
)

type Leaf struct {
	Name      string
	Schema    common.Schema
	IsRunning bool
	Store     *store.Store
	input     *chan Message
	context   context.Context
}

func NewLeaf(name string, schema common.Schema, inputFormatType store.InputFormatType, input *chan Message) (*Leaf, error) {
	ctx := context.Background()
	allocator := memory.DefaultAllocator
	s, err := store.NewInMemoryStore(&allocator, inputFormatType, &schema)
	if err != nil {
		return nil, err
	}

	leaf := Leaf{
		Name:      name,
		Schema:    schema,
		IsRunning: false,
		input:     input,
		context:   ctx,
		Store:     s,
	}

	return &leaf, nil
}

func (leaf *Leaf) Start() {
	go leaf.process()
	leaf.IsRunning = true
}

func (leaf *Leaf) Stop() {
}

func (leaf *Leaf) process() {
	for leaf.IsRunning {
		message := <-*leaf.input
		if len(message.Errors) > 0 {
			for _, err := range message.Errors {
				log.Fatalln(err)
			}

			panic(errors.New("an error occurred while consuming topic"))
		}

		(*leaf.Store).Put(message.Record.Offset, message.Record.Key, message.Record.Value)
	}
}
