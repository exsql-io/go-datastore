package store

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/csv"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/exsql-io/go-datastore/common"
	"github.com/substrait-io/substrait-go/types"
)

var DefaultGroupSize int32 = 4096

type InMemoryStore struct {
	schema          *arrow.Schema
	buffered        [][]byte
	bufferIndex     int32
	inputFormatType InputFormatType
	allocator       *memory.Allocator
	records         []arrow.Record
}

func NewInMemoryStore(allocator *memory.Allocator, inputFormatType InputFormatType, schema *common.Schema) (*Store, error) {
	arrowSchema, err := ToArrowSchema(schema)
	if err != nil {
		return nil, err
	}

	var store Store
	store = &InMemoryStore{
		schema:          arrowSchema,
		buffered:        make([][]byte, DefaultGroupSize),
		bufferIndex:     0,
		inputFormatType: inputFormatType,
		allocator:       allocator,
	}

	return &store, nil
}

func (store *InMemoryStore) Put(_ int64, _ []byte, value []byte) error {
	store.buffered[store.bufferIndex] = value
	store.bufferIndex += 1

	if store.bufferIndex == DefaultGroupSize {
		err := store.flushBuffer()
		if err != nil {
			return err
		}

		store.bufferIndex = 0
	}

	return nil
}

func (store *InMemoryStore) Iterator() (*common.CloseableIterator, error) {
	inMemoryRecords, err := store.inMemoryToRecords()
	if err != nil {
		return nil, err
	}

	return NewArrowTableCloseableIterator(inMemoryRecords, store.records), nil
}

func (store *InMemoryStore) Close() {}

func (store *InMemoryStore) Schema() *arrow.Schema {
	return store.schema
}

func (store *InMemoryStore) NamedStruct() types.NamedStruct {
	var n []string
	var t []types.Type

	for _, field := range store.schema.Fields() {
		n = append(n, field.Name)
		t = append(t, toType(field.Type))
	}

	return types.NamedStruct{
		Names: n,
		Struct: types.StructType{
			Nullability: types.NullabilityRequired,
			Types:       t,
		},
	}
}

func toType(dataType arrow.DataType) types.Type {
	switch dataType {
	case arrow.PrimitiveTypes.Int32:
		return &types.Int32Type{Nullability: types.NullabilityRequired}
	case arrow.PrimitiveTypes.Int64:
		return &types.Int64Type{Nullability: types.NullabilityRequired}
	case arrow.PrimitiveTypes.Float64:
		return &types.Float64Type{Nullability: types.NullabilityRequired}
	case arrow.PrimitiveTypes.Date32:
		return &types.DateType{Nullability: types.NullabilityRequired}
	case arrow.BinaryTypes.String:
		return &types.StringType{Nullability: types.NullabilityRequired}
	}

	return nil
}

func table(schema *arrow.Schema, records []arrow.Record) arrow.Table {
	var table arrow.Table
	table = array.NewTableFromRecords(schema, records)

	return table
}

func (store *InMemoryStore) inMemoryToRecords() (arrow.Record, error) {
	builder := array.NewRecordBuilder(*store.allocator, store.schema)
	defer builder.Release()

	switch store.inputFormatType {
	case Json:
		for index := int32(0); index < store.bufferIndex; index++ {
			err := builder.UnmarshalJSON(store.buffered[index])
			if err != nil {
				return nil, err
			}
		}

		record := builder.NewRecord()
		record.Retain()

		builder.Release()

		return record, nil
	case CSV:
		reader := csv.NewReader(
			bytes.NewReader(bytes.Join(store.buffered, []byte("\n"))),
			store.schema,
			csv.WithChunk(int(DefaultGroupSize)),
			csv.WithHeader(false))

		if !reader.Next() {
			return nil, errors.New("unable to process in memory batch")
		}

		record := reader.Record()
		record.Retain()

		reader.Release()

		return record, nil
	}

	return nil, errors.New(fmt.Sprintf("unsupported inputFormatType: '%s'", store.inputFormatType))
}

func (store *InMemoryStore) flushBuffer() error {
	record, err := store.inMemoryToRecords()
	if err != nil {
		return err
	}

	store.records = append(store.records, record)
	return nil
}
