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

func (store InMemoryStore) Iterator() (*CloseableIterator, error) {
	table, reader, err := store.reader()
	if err != nil {
		return nil, err
	}

	var iterator CloseableIterator
	iterator = inMemoryStoreCloseableIterator{
		table:  table,
		reader: reader,
	}

	return &iterator, nil
}

func (store InMemoryStore) Close() {}

func (store InMemoryStore) getOffsetFromKey(key []byte) (int64, bool) {
	offset, ok := store.keyLookup[string(key)]
	return offset, ok
}

type inMemoryStoreCloseableIterator struct {
	table  *arrow.Table
	reader *array.TableReader
}

func (iterator inMemoryStoreCloseableIterator) Next() bool {
	return iterator.reader.Next()
}

func (iterator inMemoryStoreCloseableIterator) Value() *arrow.Record {
	record := iterator.reader.Record()
	return &record
}

func (iterator inMemoryStoreCloseableIterator) Close() {
	iterator.reader.Release()
	(*iterator.table).Release()
}

func (store InMemoryStore) reader() (*arrow.Table, *array.TableReader, error) {
	record, err := store.inMemoryToRecords()
	if err != nil {
		return nil, nil, err
	}

	var table arrow.Table
	table = array.NewTableFromRecords(store.schema, []arrow.Record{*record})

	reader := array.NewTableReader(table, 64)

	return &table, reader, nil
}

func (store InMemoryStore) inMemoryToRecords() (*arrow.Record, error) {
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
		return &record, nil
	}

	return nil, errors.New(fmt.Sprintf("unsupported inputFormatType: '%s'", store.inputFormatType))
}
