package main

import (
	"fmt"
	"github.com/exsql-io/go-datastore/common"
	"github.com/exsql-io/go-datastore/store"
	"testing"
)

func TestLoadConfiguration(t *testing.T) {
	type StreamTestInformation struct {
		topic      string
		format     store.InputFormatType
		assertions []common.FieldAssertion
	}

	tests := map[string]struct {
		instanceId string
		brokers    int
		broker     string
		streams    []StreamTestInformation
	}{
		"configuration": {
			instanceId: "instance-id",
			brokers:    1,
			broker:     "localhost:9092",
			streams: []StreamTestInformation{
				{
					topic:  "nyc-taxi-trips",
					format: store.Json,
					assertions: []common.FieldAssertion{
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "vendorId", false, common.Type{Name: common.Utf8Type})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "pickupTimestamp", false, common.Type{Name: common.LongType})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "dropOffTimestamp", false, common.Type{Name: common.LongType})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "passengers", false, common.Type{Name: common.ShortType})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "distanceInMiles", false, common.Type{Name: common.DoubleType})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "paymentType", false, common.Type{Name: common.Utf8Type})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "fareAmount", false, common.Type{Name: common.DoubleType})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "tipAmount", false, common.Type{Name: common.DoubleType})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "tollsAmount", true, common.Type{Name: common.DoubleType})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "totalAmount", false, common.Type{Name: common.DoubleType})
						},
					},
				},
				{
					topic:  "tpch-lineitems",
					format: store.PSV,
					assertions: []common.FieldAssertion{
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "orderkey", false, common.Type{Name: common.LongType})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "partkey", false, common.Type{Name: common.LongType})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "suppkey", false, common.Type{Name: common.LongType})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "linenumber", false, common.Type{Name: common.LongType})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "quantity", false, common.Type{Name: common.DoubleType})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "extendedprice", false, common.Type{Name: common.DoubleType})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "discount", false, common.Type{Name: common.DoubleType})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "tax", false, common.Type{Name: common.DoubleType})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "returnflag", false, common.Type{Name: common.Utf8Type})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "linestatus", false, common.Type{Name: common.Utf8Type})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "shipdate", false, common.Type{Name: common.DateType})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "commitdate", false, common.Type{Name: common.DateType})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "receiptdate", false, common.Type{Name: common.DateType})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "shipinstruct", false, common.Type{Name: common.Utf8Type})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "shipmode", false, common.Type{Name: common.Utf8Type})
						},
						func(t *testing.T, schema *common.Schema, index int) {
							schema.AssertField(t, index, "comment", false, common.Type{Name: common.Utf8Type})
						},
					},
				},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			configuration, err := LoadConfiguration(fmt.Sprintf("testdata/yaml/%s.yaml", name))
			if err != nil {
				t.Error(err)
			}

			if configuration.InstanceId != test.instanceId {
				t.Errorf("expected InstanceId field to be 'instance-id', but got: '%s'", configuration.InstanceId)
			}

			if len(configuration.Brokers) != test.brokers {
				t.Errorf("expected 1 broker configuration, got: %d", len(configuration.Brokers))
			}

			if configuration.Brokers[0] != test.broker {
				t.Errorf("expected Brokers[0] field to be 'localhost:9092', but got: '%s'", configuration.Brokers[0])
			}

			if len(configuration.Streams) != len(test.streams) {
				t.Errorf("expected %d stream configurations, got: %d", len(test.streams), len(configuration.Streams))
			}

			for index, stream := range test.streams {
				assertStream(configuration, index, stream.topic, stream.format, stream.assertions, t)
			}
		})
	}
}

func assertStream(configuration *Configuration, index int, topic string, format store.InputFormatType, fieldAssertions []common.FieldAssertion, t *testing.T) {
	stream := configuration.Streams[index]
	if stream.Topic != topic {
		t.Errorf("expected topic of stream at: %d to be %s, got: %s", index, topic, stream.Topic)
	}

	if stream.Format != format {
		t.Errorf("expected format of stream at: %d to be %v, got: %v", index, format, stream.Format)
	}

	stream.Schema.AssertSchema(t, fieldAssertions)
}
