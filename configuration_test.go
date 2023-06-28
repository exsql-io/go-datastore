package main

import (
	"github.com/exsql-io/go-datastore/common"
	"testing"
)

func TestLoadConfiguration(t *testing.T) {
	configuration, err := LoadConfiguration("testdata/yaml/configuration.yaml")
	if err != nil {
		t.Error(err)
	}

	if configuration.InstanceId != "instance-id" {
		t.Errorf("expected InstanceId field to be 'instance-id', but got: '%s'", configuration.InstanceId)
	}

	if len(configuration.Brokers) != 1 {
		t.Errorf("expected 1 broker configuration, got: %d", len(configuration.Brokers))
	}

	if configuration.Brokers[0] != "localhost:9092" {
		t.Errorf("expected Brokers[0] field to be 'localhost:9092', but got: '%s'", configuration.Brokers[0])
	}

	if len(configuration.Streams) != 1 {
		t.Errorf("expected 1 stream configuration, got: %d", len(configuration.Streams))
	}

	assertStream(configuration, 0, "nyc-taxi-trips", Json, t)
}

func assertStream(configuration *Configuration, index int, topic string, format FormatType, t *testing.T) {
	stream := configuration.Streams[index]
	if stream.Topic != topic {
		t.Errorf("expected topic of stream at: %d to be %s, got: %s", index, topic, stream.Topic)
	}

	if stream.Format != format {
		t.Errorf("expected format of stream at: %d to be %v, got: %v", index, format, stream.Format)
	}

	stream.Schema.AssertSchema(t, []common.FieldAssertion{
		func(t *testing.T, schema *common.Schema, index int) {
			stream.Schema.AssertField(t, index, "vendorId", false, common.Type{Name: common.Utf8Type})
		},
		func(t *testing.T, schema *common.Schema, index int) {
			stream.Schema.AssertField(t, index, "pickupTimestamp", false, common.Type{Name: common.LongType})
		},
		func(t *testing.T, schema *common.Schema, index int) {
			stream.Schema.AssertField(t, index, "dropOffTimestamp", false, common.Type{Name: common.LongType})
		},
		func(t *testing.T, schema *common.Schema, index int) {
			stream.Schema.AssertField(t, index, "passengers", false, common.Type{Name: common.ShortType})
		},
		func(t *testing.T, schema *common.Schema, index int) {
			stream.Schema.AssertField(t, index, "distanceInMiles", false, common.Type{Name: common.DoubleType})
		},
		func(t *testing.T, schema *common.Schema, index int) {
			stream.Schema.AssertField(t, index, "paymentType", false, common.Type{Name: common.Utf8Type})
		},
		func(t *testing.T, schema *common.Schema, index int) {
			stream.Schema.AssertField(t, index, "fareAmount", false, common.Type{Name: common.DoubleType})
		},
		func(t *testing.T, schema *common.Schema, index int) {
			stream.Schema.AssertField(t, index, "tipAmount", false, common.Type{Name: common.DoubleType})
		},
		func(t *testing.T, schema *common.Schema, index int) {
			stream.Schema.AssertField(t, index, "tollsAmount", true, common.Type{Name: common.DoubleType})
		},
		func(t *testing.T, schema *common.Schema, index int) {
			stream.Schema.AssertField(t, index, "totalAmount", false, common.Type{Name: common.DoubleType})
		},
	})
}
