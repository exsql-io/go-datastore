package store

import (
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/exsql-io/go-datastore/common"
)

type InputFormatType string

const (
	Json InputFormatType = "json"
)

type CloseableIterator interface {
	Next() bool
	Value() *arrow.Record
	Close()
}

type Store interface {
	Get(key []byte) []byte
	Put(offset int64, key []byte, value []byte)
	Iterator() (*CloseableIterator, error)
	Close()
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
