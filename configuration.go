package main

import (
	"github.com/exsql-io/go-datastore/common"
	"gopkg.in/yaml.v3"
	"os"
)

type FormatType string

const (
	Json FormatType = "json"
)

type Stream struct {
	Topic  string        `yaml:"topic"`
	Format FormatType    `yaml:"format"`
	Schema common.Schema `yaml:"schema"`
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
