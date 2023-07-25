package store

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/compute"
	"github.com/exsql-io/go-datastore/common"
	"github.com/exsql-io/go-datastore/engine"
	"github.com/substrait-io/substrait-go/types"
)

var DefaultChunkSize int64 = 0

type InputFormatType string

const (
	Json InputFormatType = "json"
)

type arrowTableCloseableIterator struct {
	ctx             context.Context
	schema          *arrow.Schema
	inMemoryRecords arrow.Record
	records         []arrow.Record
	position        int
	table           arrow.Table
	reader          *array.TableReader
}

func (iterator *arrowTableCloseableIterator) Next() bool {
	hasNext := iterator.prepareNextNonEmptyBatch()
	if !hasNext {
		return false
	}

	if iterator.reader.Next() {
		return true
	}

	iterator.reader.Release()
	iterator.table.Release()

	return false
}

func (iterator *arrowTableCloseableIterator) Value() engine.ColumnarBatch {
	record := iterator.reader.Record()
	return &record
}

func (iterator *arrowTableCloseableIterator) Close() {
	iterator.reader.Release()
	iterator.table.Release()
}

func (iterator *arrowTableCloseableIterator) prepareNextNonEmptyBatch() bool {
	if iterator.reader == nil {
		iterator.table = table(iterator.schema, []arrow.Record{iterator.inMemoryRecords})
		iterator.reader = array.NewTableReader(iterator.table, DefaultChunkSize)

		return true
	}

	iterator.reader.Release()
	iterator.table.Release()

	iterator.position -= 1
	if iterator.position < 0 {
		return false
	}

	iterator.table = table(iterator.schema, []arrow.Record{iterator.records[iterator.position]})
	iterator.reader = array.NewTableReader(iterator.table, DefaultChunkSize)

	return true
}

func NewArrowTableCloseableIterator(inMemoryRecords arrow.Record, records []arrow.Record) *engine.CloseableIterator {
	var iterator engine.CloseableIterator
	iterator = &arrowTableCloseableIterator{
		ctx:             context.Background(),
		schema:          inMemoryRecords.Schema(),
		inMemoryRecords: inMemoryRecords,
		records:         records,
		position:        len(records),
		table:           nil,
		reader:          nil,
	}

	return &iterator
}

type Filter func(compute.Datum) (compute.Datum, error)

type Store interface {
	Put(offset int64, key []byte, value []byte) error
	Close()
	Iterator() (*engine.CloseableIterator, error)
	Schema() *arrow.Schema
	NamedStruct() types.NamedStruct
}

func ToArrowSchema(schema *common.Schema) (*arrow.Schema, error) {
	var arrowFields []arrow.Field
	for _, field := range schema.Fields {
		arrowField, err := toArrowField(&field)
		if err != nil {
			return nil, err
		}

		arrowFields = append(arrowFields, *arrowField)
	}

	return arrow.NewSchema(arrowFields, nil), nil
}

func toArrowField(field *common.Field) (*arrow.Field, error) {
	dataType, err := toArrowType(field.Type)
	if err != nil {
		return nil, err
	}

	arrowField := arrow.Field{
		Name:     field.Name,
		Nullable: field.Nullable,
		Type:     dataType,
	}

	return &arrowField, nil
}

func toArrowType(tpe common.Type) (arrow.DataType, error) {
	switch tpe.Name {
	case common.BooleanType:
		return arrow.FixedWidthTypes.Boolean, nil
	case common.ByteType:
		return arrow.PrimitiveTypes.Int8, nil
	case common.ShortType:
		return arrow.PrimitiveTypes.Int16, nil
	case common.IntType:
		return arrow.PrimitiveTypes.Int32, nil
	case common.LongType:
		return arrow.PrimitiveTypes.Int64, nil
	case common.UByteType:
		return arrow.PrimitiveTypes.Uint8, nil
	case common.UShortType:
		return arrow.PrimitiveTypes.Uint16, nil
	case common.UIntType:
		return arrow.PrimitiveTypes.Uint32, nil
	case common.ULongType:
		return arrow.PrimitiveTypes.Uint64, nil
	case common.FloatType:
		return arrow.PrimitiveTypes.Float32, nil
	case common.DoubleType:
		return arrow.PrimitiveTypes.Float64, nil
	case common.BytesType:
		return arrow.BinaryTypes.Binary, nil
	case common.Utf8Type:
		return arrow.BinaryTypes.String, nil
	default:
		return nil, errors.New(fmt.Sprintf("type: '%s' is not yet convertible to arrow type", tpe.Name))
	}
}
