package store

import (
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/exsql-io/go-datastore/common"
	"testing"
)

func TestToArrowSchema(t *testing.T) {
	schema, err := common.FromYaml("../testdata/yaml/nyc-taxi-data-schema.yaml")
	if err != nil {
		t.Error(err)
	}

	arrowSchema, err := ToArrowSchema(schema)
	if err != nil {
		t.Error(err)
	}

	if len(arrowSchema.Fields()) != len(schema.Fields) {
		t.Errorf("expected %d fields, got: %d", len(schema.Fields), len(arrowSchema.Fields()))
	}

	for index, field := range schema.Fields {
		arrowField := arrowSchema.Field(index)
		if arrowField.Name != field.Name {
			t.Errorf("expected field at %d to have name: %s, got: %s", index, field.Name, arrowField.Name)
		}

		if arrowField.Nullable != field.Nullable {
			t.Errorf("expected field at %d to have nullable: %t, got: %t", index, field.Nullable, arrowField.Nullable)
		}

		assertFieldType(t, index, field.Type.Name, arrowField.Type)
	}
}

func assertFieldType(t *testing.T, index int, name common.TypeName, dataType arrow.DataType) {
	switch name {
	case common.BooleanType:
		if dataType.ID() != arrow.BOOL {
			t.Errorf("expected field at %d to be (%d) - 'bool', got: %d - '%s'", index, arrow.BOOL, dataType.ID(), dataType.Name())
		}
	case common.ByteType:
		if dataType.ID() != arrow.INT8 {
			t.Errorf("expected field at %d to be (%d) - 'int8', got: %d - '%s'", index, arrow.INT8, dataType.ID(), dataType.Name())
		}
	case common.ShortType:
		if dataType.ID() != arrow.INT16 {
			t.Errorf("expected field at %d to be (%d) - 'int16', got: %d - '%s'", index, arrow.INT16, dataType.ID(), dataType.Name())
		}
	case common.IntType:
		if dataType.ID() != arrow.INT32 {
			t.Errorf("expected field at %d to be (%d) - 'int32', got: %d - '%s'", index, arrow.INT32, dataType.ID(), dataType.Name())
		}
	case common.LongType:
		if dataType.ID() != arrow.INT64 {
			t.Errorf("expected field at %d to be (%d) - 'int64', got: %d - '%s'", index, arrow.INT64, dataType.ID(), dataType.Name())
		}
	case common.UByteType:
		if dataType.ID() != arrow.UINT8 {
			t.Errorf("expected field at %d to be (%d) - 'uint8', got: %d - '%s'", index, arrow.UINT8, dataType.ID(), dataType.Name())
		}
	case common.UShortType:
		if dataType.ID() != arrow.UINT16 {
			t.Errorf("expected field at %d to be (%d) - 'uint16', got: %d - '%s'", index, arrow.UINT16, dataType.ID(), dataType.Name())
		}
	case common.UIntType:
		if dataType.ID() != arrow.UINT32 {
			t.Errorf("expected field at %d to be (%d) - 'uint32', got: %d - '%s'", index, arrow.UINT32, dataType.ID(), dataType.Name())
		}
	case common.ULongType:
		if dataType.ID() != arrow.UINT64 {
			t.Errorf("expected field at %d to be (%d) - 'uint64', got: %d - '%s'", index, arrow.UINT64, dataType.ID(), dataType.Name())
		}
	case common.FloatType:
		if dataType.ID() != arrow.FLOAT32 {
			t.Errorf("expected field at %d to be (%d) - 'float32', got: %d - '%s'", index, arrow.FLOAT32, dataType.ID(), dataType.Name())
		}
	case common.DoubleType:
		if dataType.ID() != arrow.FLOAT64 {
			t.Errorf("expected field at %d to be (%d) - 'float64', got: %d - '%s'", index, arrow.FLOAT64, dataType.ID(), dataType.Name())
		}
	case common.BytesType:
		if dataType.ID() != arrow.BINARY {
			t.Errorf("expected field at %d to be (%d) - 'binary', got: %d - '%s'", index, arrow.BINARY, dataType.ID(), dataType.Name())
		}
	case common.Utf8Type:
		if dataType.ID() != arrow.STRING {
			t.Errorf("expected field at %d to be (%d) - 'string', got: %d - '%s'", index, arrow.STRING, dataType.ID(), dataType.Name())
		}
	default:
		t.Errorf("unexpected type: %+v", dataType)
	}
}
