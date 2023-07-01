package main

import (
	"github.com/exsql-io/go-datastore/common"
	"github.com/exsql-io/go-datastore/store"
	"gopkg.in/yaml.v3"
	"os"
)

type Stream struct {
	Topic  string                `yaml:"topic"`
	Format store.InputFormatType `yaml:"format"`
	Schema common.Schema         `yaml:"schema"`
}

type Configuration struct {
	InstanceId string `yaml:"instanceId"`
	Brokers    []string
	Streams    []Stream `yaml:"streams"`
}

func LoadConfiguration(path string) (*Configuration, error) {
	configuration := Configuration{}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(data, &configuration)
	if err != nil {
		return nil, err
	}

	return &configuration, nil
}
