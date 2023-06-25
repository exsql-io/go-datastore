package services

import (
	"context"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Message struct {
	Errors []error
	Record *kgo.Record
}

type Tailer struct {
	Id        string
	Topic     string
	Channel   chan Message
	IsRunning bool
	context   context.Context
	client    *kgo.Client
}

func NewTailer(id string, brokers []string, topic string) (*Tailer, error) {
	ctx := context.Background()
	channel := make(chan Message)
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ClientID(id),
	)

	if err != nil {
		return nil, err
	}

	tailer := Tailer{
		Id:        id,
		Topic:     topic,
		Channel:   channel,
		IsRunning: false,
		context:   ctx,
		client:    client,
	}

	return &tailer, nil
}

func (tailer *Tailer) Start() {
	go consume(tailer.client, tailer.Channel, tailer.context)
	tailer.IsRunning = true
}

func (tailer *Tailer) Stop() {
	tailer.IsRunning = false
	tailer.client.Close()
}

func consume(client *kgo.Client, publishTo chan Message, context context.Context) {
	for {
		fetches := client.PollFetches(context)
		if errs := fetches.Errors(); len(errs) > 0 {
			errors := make([]error, len(errs))
			for i, err := range errs {
				errors[i] = err.Err
			}

			publishTo <- Message{Errors: errors}
			return
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			publishTo <- Message{Record: iter.Next()}
		}
	}
}
