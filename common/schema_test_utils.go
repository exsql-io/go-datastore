package common

import (
	"testing"
)

type FieldAssertion func(t *testing.T, schema *Schema, index int)

func (schema *Schema) AssertSchema(t *testing.T, assertions []FieldAssertion) {
	if len(schema.Fields) != len(assertions) {
		t.Errorf("expected %d fields, got: %d", len(assertions), len(schema.Fields))
	}

	for index, assertion := range assertions {
		assertion(t, schema, index)
	}
}

func (schema *Schema) AssertField(t *testing.T, index int, name string, nullable bool, tpe Type) {
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
