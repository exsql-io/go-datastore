package services

import (
	"context"
	"github.com/twmb/franz-go/pkg/kgo"
	"sync"
)

type Message struct {
	Errors []error
	Record *kgo.Record
}

type Tailer struct {
	Id        string
	Topic     string
	Channel   *chan Message
	IsRunning bool
	context   context.Context
	client    *kgo.Client
	wg        *sync.WaitGroup
}

func NewTailer(id string, brokers []string, topic string, wg *sync.WaitGroup) (*Tailer, error) {
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
		Channel:   &channel,
		IsRunning: false,
		context:   ctx,
		client:    client,
		wg:        wg,
	}

	return &tailer, nil
}

func (tailer *Tailer) Start() {
	go tailer.consume()
	tailer.IsRunning = true
	tailer.wg.Add(1)
}

func (tailer *Tailer) Stop() {
	tailer.IsRunning = false
	tailer.client.Close()
	tailer.wg.Done()
}

func (tailer *Tailer) consume() {
	for tailer.IsRunning {
		fetches := tailer.client.PollFetches(tailer.context)
		if errs := fetches.Errors(); len(errs) > 0 {
			errors := make([]error, len(errs))
			for i, err := range errs {
				errors[i] = err.Err
			}

			*tailer.Channel <- Message{Errors: errors}
			return
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			*tailer.Channel <- Message{Record: iter.Next()}
		}
	}
}
