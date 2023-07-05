package store

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/compute"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/exsql-io/go-datastore/common"
)

type InMemoryStore struct {
	schema          *arrow.Schema
	records         map[int64][]byte
	keyLookup       map[string]int64
	inputFormatType InputFormatType
	allocator       *memory.Allocator
}

func NewInMemoryStore(allocator *memory.Allocator, inputFormatType InputFormatType, schema *common.Schema) (*Store, error) {
	arrowSchema, err := ToArrowSchema(schema)
	if err != nil {
		return nil, err
	}

	var store Store
	store = InMemoryStore{
		schema:          arrowSchema,
		records:         map[int64][]byte{},
		keyLookup:       map[string]int64{},
		inputFormatType: inputFormatType,
		allocator:       allocator,
	}

	return &store, nil
}

func (store InMemoryStore) Get(key []byte) []byte {
	offset, ok := store.getOffsetFromKey(key)
	if !ok {
		return nil
	}

	value, ok := store.records[offset]
	if !ok {
		return nil
	}

	return value
}

func (store InMemoryStore) Put(offset int64, key []byte, value []byte) {
	store.records[offset] = value
	store.keyLookup[string(key)] = offset
}

func (store InMemoryStore) Iterator(filter ...Filter) (*CloseableIterator, error) {
	records, err := store.inMemoryToRecords()
	if err != nil {
		return nil, err
	}

	if len(filter) == 1 {
		recordDatum := compute.NewDatum(records)
		datum, err := filter[0](recordDatum)
		if err != nil {
			return nil, err
		}

		dtm := datum.(*compute.ArrayDatum)

		recs, err := compute.FilterRecordBatch(context.Background(), records, dtm.MakeArray(), compute.DefaultFilterOptions())

		tbl := table(store.schema, &recs)
		reader := array.NewTableReader(tbl, 64)
		return NewArrowTableCloseableIterator(&tbl, reader), nil
	}

	tbl := table(store.schema, &records)
	reader := array.NewTableReader(tbl, 64)
	return NewArrowTableCloseableIterator(&tbl, reader), nil
}

func (store InMemoryStore) Close() {}

func (store InMemoryStore) Schema() *arrow.Schema {
	return store.schema
}

func (store InMemoryStore) getOffsetFromKey(key []byte) (int64, bool) {
	offset, ok := store.keyLookup[string(key)]
	return offset, ok
}

func table(schema *arrow.Schema, records *arrow.Record) arrow.Table {
	var table arrow.Table
	table = array.NewTableFromRecords(schema, []arrow.Record{*records})

	return table
}

func (store InMemoryStore) inMemoryToRecords() (arrow.Record, error) {
	builder := array.NewRecordBuilder(*store.allocator, store.schema)
	defer builder.Release()

	switch store.inputFormatType {
	case Json:
		for _, value := range store.records {
			err := builder.UnmarshalJSON(value)
			if err != nil {
				return nil, err
			}
		}

		record := builder.NewRecord()
		return record, nil
	}

	return nil, errors.New(fmt.Sprintf("unsupported inputFormatType: '%s'", store.inputFormatType))
}
