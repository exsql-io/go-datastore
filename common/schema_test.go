package common

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

	schema.AssertSchema(t, []FieldAssertion{
		func(t *testing.T, schema *Schema, index int) {
			schema.AssertField(t, index, "vendorId", false, Type{Name: Utf8Type})
		},
		func(t *testing.T, schema *Schema, index int) {
			schema.AssertField(t, index, "pickupTimestamp", false, Type{Name: LongType})
		},
		func(t *testing.T, schema *Schema, index int) {
			schema.AssertField(t, index, "dropOffTimestamp", false, Type{Name: LongType})
		},
		func(t *testing.T, schema *Schema, index int) {
			schema.AssertField(t, index, "passengers", false, Type{Name: ShortType})
		},
		func(t *testing.T, schema *Schema, index int) {
			schema.AssertField(t, index, "distanceInMiles", false, Type{Name: DoubleType})
		},
		func(t *testing.T, schema *Schema, index int) {
			schema.AssertField(t, index, "paymentType", false, Type{Name: Utf8Type})
		},
		func(t *testing.T, schema *Schema, index int) {
			schema.AssertField(t, index, "fareAmount", false, Type{Name: DoubleType})
		},
		func(t *testing.T, schema *Schema, index int) {
			schema.AssertField(t, index, "tipAmount", false, Type{Name: DoubleType})
		},
		func(t *testing.T, schema *Schema, index int) {
			schema.AssertField(t, index, "tollsAmount", true, Type{Name: DoubleType})
		},
		func(t *testing.T, schema *Schema, index int) {
			schema.AssertField(t, index, "totalAmount", false, Type{Name: DoubleType})
		},
	})
}
