package services

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
)

type Leaf struct {
	Schema    Schema
	IsRunning bool
	input     *chan Message
	context   context.Context
	wg        *sync.WaitGroup
}

func NewLeaf(schema Schema, input *chan Message, wg *sync.WaitGroup) (Leaf, error) {
	ctx := context.Background()
	leaf := Leaf{
		Schema:    schema,
		IsRunning: false,
		input:     input,
		context:   ctx,
		wg:        wg,
	}

	return leaf, nil
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

		fmt.Println("topic:", message.Record.Topic, "record value:", string(message.Record.Value))
	}
}
