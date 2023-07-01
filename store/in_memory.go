package store

import (
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/exsql-io/go-datastore/common"
)

type InMemoryStore struct {
	_schema         *arrow.Schema
	records         []arrow.Record
	inputFormatType InputFormatType
	allocator       *memory.Allocator
}

func NewInMemoryStore(allocator *memory.Allocator, inputFormatType InputFormatType, schema *common.Schema) (*InMemoryStore, error) {
	arrowSchema, err := ToArrowSchema(schema)
	if err != nil {
		return nil, err
	}

	store := InMemoryStore{
		_schema:         arrowSchema,
		records:         []arrow.Record{},
		inputFormatType: inputFormatType,
		allocator:       allocator,
	}

	return &store, nil
}

func (store *InMemoryStore) schema() *arrow.Schema {
	return store._schema
}

func (store *InMemoryStore) Close() {}

func (store *InMemoryStore) Iterator() InMemoryStoreCloseableIterator {
	return InMemoryStoreCloseableIterator{
		store: store,
		index: -1,
	}
}

func (store *InMemoryStore) Append(data []byte) error {
	builder := array.NewRecordBuilder(*store.allocator, store._schema)
	defer builder.Release()

	switch store.inputFormatType {
	case Json:
		err := builder.UnmarshalJSON(data)
		if err != nil {
			return err
		}

		store.records = append(store.records, builder.NewRecord())
		return nil
	}

	return errors.New(fmt.Sprintf("unsupported inputFormatType: %s", store.inputFormatType))
}

type InMemoryStoreCloseableIterator struct {
	store *InMemoryStore
	index int
}

func (closeableIterator *InMemoryStoreCloseableIterator) Close() {}

func (closeableIterator *InMemoryStoreCloseableIterator) Value() *arrow.Record {
	return &closeableIterator.store.records[closeableIterator.index]
}

func (closeableIterator *InMemoryStoreCloseableIterator) Next() bool {
	closeableIterator.index++

	records := len(closeableIterator.store.records)
	if records == 0 || closeableIterator.index == records {
		return false
	}

	return true
}
