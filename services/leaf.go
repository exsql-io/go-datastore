package services

import (
	"errors"
	"fmt"
	"log"
)

type Leaf struct {
	Schema *Schema
	input  chan Message
}

func NewLeaf(schema Schema, input chan Message) (Leaf, error) {
	leaf := Leaf{
		Schema: &schema,
		input:  input,
	}

	return leaf, nil
}

func (leaf *Leaf) Start() {
	go process(leaf.Schema, leaf.input)
}

func (leaf *Leaf) Stop() {

}

func process(schema *Schema, input chan Message) {
	for {
		message := <-input
		if len(message.Errors) > 0 {
			for _, err := range message.Errors {
				log.Fatalln(err)
			}

			panic(errors.New("an error occurred while consuming topic"))
		}

		fmt.Println("topic:", message.Record.Topic, "record value:", string(message.Record.Value))
	}
}
