package services

import (
	"context"
	"errors"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/exsql-io/go-datastore/common"
	"github.com/exsql-io/go-datastore/store"
	"log"
	"sync"
)

type Leaf struct {
	Schema    common.Schema
	IsRunning bool
	Store     *store.Store
	input     *chan Message
	context   context.Context
	wg        *sync.WaitGroup
}

func NewLeaf(schema common.Schema, inputFormatType store.InputFormatType, input *chan Message, wg *sync.WaitGroup) (*Leaf, error) {
	ctx := context.Background()
	allocator := memory.DefaultAllocator
	s, err := store.NewInMemoryStore(&allocator, inputFormatType, &schema)
	if err != nil {
		return nil, err
	}

	leaf := Leaf{
		Schema:    schema,
		IsRunning: false,
		input:     input,
		context:   ctx,
		wg:        wg,
		Store:     s,
	}

	return &leaf, nil
}

func (leaf *Leaf) Start() {
	go leaf.process()
	leaf.IsRunning = true
	leaf.wg.Add(1)
}

func (leaf *Leaf) Stop() {
	leaf.wg.Done()
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
