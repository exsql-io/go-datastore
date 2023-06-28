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

	AssertSchema(t, schema, []FieldAssertion{
		func(t *testing.T, schema *Schema, index int) {
			assertField(t, schema, index, "vendorId", false, Type{Name: Utf8Type})
		},
		func(t *testing.T, schema *Schema, index int) {
			assertField(t, schema, index, "pickupTimestamp", false, Type{Name: LongType})
		},
		func(t *testing.T, schema *Schema, index int) {
			assertField(t, schema, index, "dropOffTimestamp", false, Type{Name: LongType})
		},
		func(t *testing.T, schema *Schema, index int) {
			assertField(t, schema, index, "passengers", false, Type{Name: ShortType})
		},
		func(t *testing.T, schema *Schema, index int) {
			assertField(t, schema, index, "distanceInMiles", false, Type{Name: DoubleType})
		},
		func(t *testing.T, schema *Schema, index int) {
			assertField(t, schema, index, "paymentType", false, Type{Name: Utf8Type})
		},
		func(t *testing.T, schema *Schema, index int) {
			assertField(t, schema, index, "fareAmount", false, Type{Name: DoubleType})
		},
		func(t *testing.T, schema *Schema, index int) {
			assertField(t, schema, index, "tipAmount", false, Type{Name: DoubleType})
		},
		func(t *testing.T, schema *Schema, index int) {
			assertField(t, schema, index, "tollsAmount", true, Type{Name: DoubleType})
		},
		func(t *testing.T, schema *Schema, index int) {
			assertField(t, schema, index, "totalAmount", false, Type{Name: DoubleType})
		},
	})
}

type FieldAssertion func(t *testing.T, schema *Schema, index int)

func AssertSchema(t *testing.T, schema *Schema, assertions []FieldAssertion) {
	if len(schema.Fields) != len(assertions) {
		t.Errorf("expected %d fields, got: %d", len(assertions), len(schema.Fields))
	}

	for index, assertion := range assertions {
		assertion(t, schema, index)
	}
}

func assertField(t *testing.T, schema *Schema, index int, name string, nullable bool, tpe Type) {
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
