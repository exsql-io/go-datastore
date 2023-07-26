package common

import (
	"gopkg.in/yaml.v3"
	"os"
)

type TypeName string

const (
	BooleanType   TypeName = "boolean"
	ByteType      TypeName = "byte"
	ShortType     TypeName = "short"
	IntType       TypeName = "int"
	LongType      TypeName = "long"
	UByteType     TypeName = "ubyte"
	UShortType    TypeName = "ushort"
	UIntType      TypeName = "uint"
	ULongType     TypeName = "ulong"
	FloatType     TypeName = "float"
	DoubleType    TypeName = "double"
	BytesType     TypeName = "bytes"
	Utf8Type      TypeName = "utf8"
	DateType      TypeName = "date"
	ArrayType     TypeName = "array"
	StructureType TypeName = "structure"
)

// Type is either a scalar type, for which TypeName will be:
// - BooleanType
// - ByteType
// - ShortType
// - IntType
// - LongType
// - UByteType
// - UShortType
// - UIntType
// - ULongType
// - FloatType
// - DoubleType
// - BytesType
// - Utf8Type
// - DateType
// and Values, Fields will be nil
// or an ArrayType (TypeName will be ArrayType), Values will be another Type definition and Fields will be nil
// or a StructureType (TypeName will be StructureType), Values will be nil and Fields will be set with the fields of the structure.
type Type struct {
	Name   TypeName
	Values *Type
	Fields *Fields
}

type Field struct {
	Name     string
	Nullable bool
	Type     Type
	Metadata map[string]string
}

type Fields []Field

type Schema struct {
	Fields Fields
}

func FromYaml(path string) (*Schema, error) {
	yml, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	schema := Schema{}
	err = yaml.Unmarshal(yml, &schema)
	if err != nil {
		return nil, err
	}

	return &schema, nil
}
