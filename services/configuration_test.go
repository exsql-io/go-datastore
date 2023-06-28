package services

import (
	"testing"
)

func TestLoadConfiguration(t *testing.T) {
	configuration, err := LoadConfiguration("../testdata/yaml/configuration.yaml")
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

	AssertSchema(t, &stream.Schema, []FieldAssertion{
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
