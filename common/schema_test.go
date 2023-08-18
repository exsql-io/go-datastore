package common

import (
	"fmt"
	"testing"
)

func TestFromYaml(t *testing.T) {
	tests := map[string]struct {
		assertions []FieldAssertion
	}{
		"nyc-taxi-data-schema": {
			assertions: []FieldAssertion{
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
			},
		},
		"tpch-customer-schema": {
			assertions: []FieldAssertion{
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "custkey", false, Type{Name: LongType})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "name", false, Type{Name: Utf8Type})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "address", false, Type{Name: Utf8Type})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "nationkey", false, Type{Name: LongType})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "phone", false, Type{Name: Utf8Type})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "acctbal", false, Type{Name: DoubleType})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "mktsegment", false, Type{Name: Utf8Type})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "comment", false, Type{Name: Utf8Type})
				},
			},
		},
		"tpch-lineitem-schema": {
			assertions: []FieldAssertion{
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "orderkey", false, Type{Name: LongType})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "partkey", false, Type{Name: LongType})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "suppkey", false, Type{Name: LongType})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "linenumber", false, Type{Name: LongType})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "quantity", false, Type{Name: DoubleType})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "extendedprice", false, Type{Name: DoubleType})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "discount", false, Type{Name: DoubleType})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "tax", false, Type{Name: DoubleType})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "returnflag", false, Type{Name: Utf8Type})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "linestatus", false, Type{Name: Utf8Type})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "shipdate", false, Type{Name: DateType})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "commitdate", false, Type{Name: DateType})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "receiptdate", false, Type{Name: DateType})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "shipinstruct", false, Type{Name: Utf8Type})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "shipmode", false, Type{Name: Utf8Type})
				},
				func(t *testing.T, schema *Schema, index int) {
					schema.AssertField(t, index, "comment", false, Type{Name: Utf8Type})
				},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			schema, err := FromYaml(fmt.Sprintf("../testdata/yaml/%s.yaml", name))
			if err != nil {
				t.Error(err)
			}

			schema.AssertSchema(t, test.assertions)
		})
	}
}
