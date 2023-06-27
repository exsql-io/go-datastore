package services

import (
	"os"
	"testing"
)

func TestFromYaml(t *testing.T) {
	yml, err := os.ReadFile("../testdata/yaml/schema.yaml")
	if err != nil {
		t.Error(err)
	}

	schema, err := FromYaml(yml)
	if err != nil {
		t.Error(err)
	}

	if len(schema.Fields) != 10 {
		t.Errorf("expected 10 fields, got: %d", len(schema.Fields))
	}

	assertField(schema, 0, "vendorId", false, Type{Name: Utf8Type}, t)
	assertField(schema, 1, "pickupTimestamp", false, Type{Name: LongType}, t)
	assertField(schema, 2, "dropOffTimestamp", false, Type{Name: LongType}, t)
	assertField(schema, 3, "passengers", false, Type{Name: ShortType}, t)
	assertField(schema, 4, "distanceInMiles", false, Type{Name: DoubleType}, t)
	assertField(schema, 5, "paymentType", false, Type{Name: Utf8Type}, t)
	assertField(schema, 6, "fareAmount", false, Type{Name: DoubleType}, t)
	assertField(schema, 7, "tipAmount", false, Type{Name: DoubleType}, t)
	assertField(schema, 8, "tollsAmount", true, Type{Name: DoubleType}, t)
	assertField(schema, 9, "totalAmount", false, Type{Name: DoubleType}, t)
}

func assertField(schema *Schema, index int, name string, nullable bool, tpe Type, t *testing.T) {
	field := schema.Fields[index]
	if field.Name != name {
		t.Errorf("expected name of field at: %d to be %s, got: %s", index, name, field.Name)
	}

	if field.Nullable != nullable {
		t.Errorf("expected nullable of field at: %d to be %v, got: %v", index, nullable, field.Nullable)
	}

	if field.Type != tpe {
		t.Errorf("expected type of field at: %d to be %+v, got: %+v", index, tpe, field.Type)
	}
}
